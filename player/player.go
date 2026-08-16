package player

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"time"

	librespot "github.com/devgianlu/go-librespot"
	"github.com/devgianlu/go-librespot/audio"
	"github.com/devgianlu/go-librespot/cache"
	"github.com/devgianlu/go-librespot/flac"
	"github.com/devgianlu/go-librespot/mp3"
	"github.com/devgianlu/go-librespot/output"
	"github.com/devgianlu/go-librespot/playplay"
	downloadpb "github.com/devgianlu/go-librespot/proto/spotify/download"
	extmetadatapb "github.com/devgianlu/go-librespot/proto/spotify/extendedmetadata"
	audiofilespb "github.com/devgianlu/go-librespot/proto/spotify/extendedmetadata/audiofiles"
	metadatapb "github.com/devgianlu/go-librespot/proto/spotify/metadata"
	streamingpb "github.com/devgianlu/go-librespot/proto/spotify/streaming"
	"github.com/devgianlu/go-librespot/spclient"
	"github.com/devgianlu/go-librespot/vorbis"
	"golang.org/x/exp/rand"
)

const (
	SampleRate = 44100
	Channels   = 2
)

const MaxStateVolume = 65535

const CdnUrlQuarantineDuration = 15 * time.Minute

var ErrPlayerClosed = errors.New("player is closed")

func ptr[T any](v T) *T {
	return &v
}

type Player struct {
	log librespot.Logger

	crossfadeSamples          int
	flacEnabled               bool
	normalisationEnabled      bool
	normalisationUseAlbumGain bool
	normalisationPregain      float32
	countryCode               *string

	sp       *spclient.Spclient
	audioKey *audio.KeyProvider
	events   EventManager

	cache *cache.Cache

	cdnQuarantine map[string]time.Time

	newOutput func(source librespot.Float32Reader, volume float32, device string) (output.Output, error)

	// defaultAudioDevice is the output device to open initially. manageLoop
	// tracks the current device from here and it can be changed at runtime via
	// ReopenOutput.
	defaultAudioDevice string

	cmd    chan playerCmd
	ev     chan Event
	closed chan struct{}

	volumeSteps uint32

	startedPlaying time.Time
}

type playerCmdType int

const (
	playerCmdSet playerCmdType = iota
	playerCmdPlay
	playerCmdPause
	playerCmdStop
	playerCmdSeek
	playerCmdPosition
	playerCmdVolume
	playerCmdReopenOutput
	playerCmdClose
)

type playerCmd struct {
	typ  playerCmdType
	data any
	resp chan any
}

type playerCmdDataSet struct {
	source  librespot.AudioSource
	primary bool
	paused  bool
	drop    bool
}

type Options struct {
	Spclient *spclient.Spclient
	AudioKey *audio.KeyProvider
	Events   EventManager

	Log librespot.Logger

	// Cache, when non-nil, is used to store and retrieve encrypted audio files
	// on disk, avoiding a CDN download when a track is played again.
	Cache *cache.Cache

	// FlacEnabled specifies if FLAC files should be preferred when available.
	// When setting this to true, it is assumed that the PlayPlay plugin is provided.
	FlacEnabled bool

	// NormalisationEnabled specifies if the volume should be normalised according
	// to Spotify parameters. Only track normalization is supported.
	NormalisationEnabled bool
	// NormalisationUseAlbumGain specifies whether album gain instead of track gain
	// should be used for normalisation
	NormalisationUseAlbumGain bool
	// NormalisationPregain specifies the pre-gain to apply when normalising the volume
	// in dB. Use negative values to avoid clipping.
	NormalisationPregain float32

	// CrossfadeDuration specifies for how long tracks should overlap during
	// a track change. Zero disables crossfading.
	CrossfadeDuration time.Duration

	// CountryCode specifies the country code to use for media restrictions.
	CountryCode *string

	// AudioBackend specifies the audio backend to use (alsa, pulseaudio, etc).
	AudioBackend string
	// AudioBackendRuntimeSocket specifies a prefixed with protocol (e.g. `unix:` or `tcp:`) path
	// to a runtime socket of audio backend.
	//
	// This feature is support only for the pulseaudio backend.
	AudioBackendRuntimeSocket string
	// AudioDevice specifies the audio device name.
	//
	// This feature is support only for the alsa and pulseaudio backend.
	AudioDevice string
	// MixerDevice specifies the audio mixer name.
	//
	// This feature is support only for the alsa backend.
	MixerDevice string
	// MixerControlName specifies the mixer control name.
	//
	// This only works in combination with Mixer.
	MixerControlName string
	// AudioBufferTime is the buffer time in microseconds.
	//
	// This is only supported on the alsa backend.
	AudioBufferTime int
	// AudioPeriodCount is the number of periods to request.
	//
	// This is only supported on the alsa backend.
	AudioPeriodCount int

	// ExternalVolume specifies, if the volume is controlled outside the app.
	//
	// This is only supported on the alsa and pipe backends.
	// The PulseAudio backend always uses external volume.
	ExternalVolume bool

	// VolumeUpdate is a channel on which volume updates will be sent back to
	// Spotify. This must be a buffered channel.
	VolumeUpdate chan float32

	// AudioOutputPipe is the path to the output pipe.
	//
	// This is only supported on the pipe backend.
	AudioOutputPipe string

	// AudioOutputPipeFormat is the format of the output pipe.
	// Available formats are: "s16le", "s32le", "f32le". Default is "s16le".
	//
	// This is only supported on the pipe backend.
	AudioOutputPipeFormat string

	// AudioOutputPipeWaitForReader makes the pipe backend wait for a reader to
	// appear when opening the FIFO, instead of failing if none is present at the
	// time playback starts. This is useful for readers (e.g. snapcast with
	// dryout) that only connect to the FIFO when data is expected.
	//
	// This is only supported on the pipe backend.
	AudioOutputPipeWaitForReader bool
}

func NewPlayer(opts *Options) (*Player, error) {
	p := &Player{
		log:                       opts.Log,
		crossfadeSamples:          int(opts.CrossfadeDuration*SampleRate/time.Second) * Channels,
		sp:                        opts.Spclient,
		audioKey:                  opts.AudioKey,
		events:                    opts.Events,
		cache:                     opts.Cache,
		cdnQuarantine:             make(map[string]time.Time),
		flacEnabled:               opts.FlacEnabled,
		normalisationEnabled:      opts.NormalisationEnabled,
		normalisationUseAlbumGain: opts.NormalisationUseAlbumGain,
		normalisationPregain:      opts.NormalisationPregain,
		countryCode:               opts.CountryCode,
		defaultAudioDevice:        opts.AudioDevice,
		newOutput: func(reader librespot.Float32Reader, volume float32, device string) (output.Output, error) {
			return output.NewOutput(&output.NewOutputOptions{
				Log:                     opts.Log,
				Backend:                 opts.AudioBackend,
				Reader:                  reader,
				SampleRate:              SampleRate,
				ChannelCount:            Channels,
				Device:                  device,
				RuntimeSocket:           opts.AudioBackendRuntimeSocket,
				Mixer:                   opts.MixerDevice,
				Control:                 opts.MixerControlName,
				InitialVolume:           volume,
				BufferTimeMicro:         opts.AudioBufferTime,
				PeriodCount:             opts.AudioPeriodCount,
				ExternalVolume:          opts.ExternalVolume,
				VolumeUpdate:            opts.VolumeUpdate,
				OutputPipe:              opts.AudioOutputPipe,
				OutputPipeFormat:        opts.AudioOutputPipeFormat,
				OutputPipeWaitForReader: opts.AudioOutputPipeWaitForReader,
			})
		},

		cmd:    make(chan playerCmd),
		ev:     make(chan Event, 128),
		closed: make(chan struct{}),
	}

	go p.manageLoop()

	return p, nil
}

func (p *Player) manageLoop() {
	// currently available output device
	var out output.Output
	outErr := make(<-chan error)

	// initial volume is 1
	volume := float32(1)

	// whether the output is paused, so seek knows whether to resume after Drop
	paused := false

	// current output device; can be changed at runtime via playerCmdReopenOutput
	device := p.defaultAudioDevice

	// init main source
	source := NewSwitchingAudioSource(p.crossfadeSamples)

loop:
	for {
		select {
		case cmd := <-p.cmd:
			switch cmd.typ {
			case playerCmdSet:
				data := cmd.data.(playerCmdDataSet)
				if !data.primary {
					source.SetSecondary(data.source)
					cmd.resp <- nil
					break
				}

				// create a new output device if needed
				if out == nil {
					var err error
					out, err = p.newOutput(source, volume, device)
					if err != nil {
						cmd.resp <- err
						break
					}

					outErr = out.Error()
					p.log.Debugf("created new output device")
				}

				// Flush the previous source before switching, otherwise the
				// new track's opening is buffered then dropped, clipping it on
				// pulseaudio (#292).
				if data.drop {
					_ = out.Drop()
				}

				// set source
				source.SetPrimary(data.source)
				if data.paused {
					if err := out.Pause(); err != nil {
						cmd.resp <- err
						break
					}
				} else {
					if err := out.Resume(); err != nil {
						cmd.resp <- err
						break
					}
				}
				paused = data.paused

				p.startedPlaying = time.Now()
				cmd.resp <- nil

				if data.paused {
					p.ev <- Event{Type: EventTypePause}
				} else {
					p.ev <- Event{Type: EventTypePlay}
				}
			case playerCmdPlay:
				if out != nil {
					if err := out.Resume(); err != nil {
						cmd.resp <- err
					} else {
						paused = false
						cmd.resp <- nil
						p.ev <- Event{Type: EventTypeResume}
					}
				} else {
					paused = false
					cmd.resp <- nil
				}
			case playerCmdPause:
				if out != nil {
					if err := out.Pause(); err != nil {
						cmd.resp <- err
					} else {
						paused = true
						cmd.resp <- nil
						p.ev <- Event{Type: EventTypePause}
					}
				} else {
					paused = true
					cmd.resp <- nil
				}
			case playerCmdStop:
				if out != nil {
					_ = out.Close()
					out = nil
					outErr = make(<-chan error)

					p.log.Tracef("closed output device because of stop command")
				}

				cmd.resp <- struct{}{}
				p.ev <- Event{Type: EventTypeStop}
			case playerCmdSeek:
				if out != nil {
					if err := source.SetPositionMs(cmd.data.(int64)); err != nil {
						cmd.resp <- err
					} else if err = out.Drop(); err != nil {
						cmd.resp <- err
					} else {
						// Drop no longer restarts the stream; resume unless paused.
						if !paused {
							err = out.Resume()
						}
						cmd.resp <- err
					}
				} else {
					cmd.resp <- nil
				}
			case playerCmdPosition:
				pos := source.PositionMs()
				if out != nil {
					delay, err := out.DelayMs()
					if err != nil {
						p.log.WithError(err).Warnf("failed getting output device delay")
						delay = 0
					}

					cmd.resp <- pos - delay
				} else {
					cmd.resp <- pos
				}
			case playerCmdVolume:
				volume = cmd.data.(float32)
				if out != nil {
					out.SetVolume(volume)
				}
			case playerCmdReopenOutput:
				// Reopen the output on a new device without touching the Spotify
				// session, keeping the current source and playback position.
				device = cmd.data.(string)

				if out == nil {
					// Nothing playing: the new device takes effect the next time
					// an output is opened. Callers wanting audio to resume (e.g.
					// recovery after the device died) should issue a play/resume
					// afterwards.
					cmd.resp <- nil
					break
				}

				// Close the old device before opening the new one: some backends
				// (e.g. pulseaudio) start reading from the source as soon as the
				// output is constructed, so two live outputs would corrupt the
				// stream. A sub-second gap from the discarded buffer is expected.
				_ = out.Drop()
				_ = out.Close()
				out = nil
				outErr = make(<-chan error)

				newOut, err := p.newOutput(source, volume, device)
				if err != nil {
					// The old device is already gone; playback stays silent until
					// an output is reopened. Surface the failure to the caller.
					p.log.WithError(err).Warnf("failed reopening output on %q", device)
					cmd.resp <- err
					break
				}

				out = newOut
				outErr = out.Error()

				if paused {
					err = out.Pause()
				} else {
					err = out.Resume()
				}
				if err != nil {
					_ = out.Close()
					out = nil
					outErr = make(<-chan error)
					cmd.resp <- err
					break
				}

				p.log.Infof("reopened output device on %q", device)
				cmd.resp <- nil
			case playerCmdClose:
				break loop
			default:
				panic("unknown player command")
			}
		case err := <-outErr:
			if err != nil {
				p.log.WithError(err).Errorf("output device failed")
			}

			// the current output device has exited, clean it up
			_ = out.Close()
			out = nil
			outErr = make(<-chan error)

			p.log.Tracef("cleared closed output device")

			// FIXME: this is called even if not needed, like when autoplay starts
			p.ev <- Event{Type: EventTypeStop}
		case <-source.Done():
			p.ev <- Event{Type: EventTypeNotPlaying}
		}
	}

	close(p.closed)

	_ = source.Close()

	if out != nil {
		_ = out.Close()
		out = nil
	}
}

func (p *Player) HasBeenPlayingFor() time.Duration {
	if p.startedPlaying.IsZero() {
		return 0
	}

	return time.Since(p.startedPlaying)
}

func (p *Player) Receive() <-chan Event {
	return p.ev
}

// send hands cmd to manageLoop, reporting false if the player has already been
// closed and the command was therefore not delivered. Once the send succeeds
// manageLoop always answers cmd.resp before it can exit, so callers may wait on
// the response unconditionally.
func (p *Player) send(cmd playerCmd) bool {
	select {
	case p.cmd <- cmd:
		return true
	case <-p.closed:
		return false
	}
}

// Close stops the player. It is safe to call more than once and safe to call
// while other goroutines are issuing commands: those get ErrPlayerClosed.
func (p *Player) Close() {
	p.send(playerCmd{typ: playerCmdClose})
}

func (p *Player) SetVolume(val uint32) {
	vol := float32(val) / MaxStateVolume
	p.send(playerCmd{typ: playerCmdVolume, data: vol})
}

func (p *Player) Play() error {
	resp := make(chan any, 1)
	if !p.send(playerCmd{typ: playerCmdPlay, resp: resp}) {
		return ErrPlayerClosed
	}
	if err := <-resp; err != nil {
		return err.(error)
	}

	return nil
}

func (p *Player) Pause() error {
	resp := make(chan any, 1)
	if !p.send(playerCmd{typ: playerCmdPause, resp: resp}) {
		return ErrPlayerClosed
	}
	if err := <-resp; err != nil {
		return err.(error)
	}

	return nil
}

func (p *Player) Stop() {
	resp := make(chan any, 1)
	if !p.send(playerCmd{typ: playerCmdStop, resp: resp}) {
		return
	}
	<-resp
}

func (p *Player) SeekMs(pos int64) error {
	resp := make(chan any, 1)
	if !p.send(playerCmd{typ: playerCmdSeek, data: pos, resp: resp}) {
		return ErrPlayerClosed
	}
	if err := <-resp; err != nil {
		return err.(error)
	}

	return nil
}

// PositionMs returns the current playback position, or zero if the player has
// been closed.
func (p *Player) PositionMs() int64 {
	resp := make(chan any, 1)
	if !p.send(playerCmd{typ: playerCmdPosition, resp: resp}) {
		return 0
	}
	pos := <-resp
	return pos.(int64)
}

// ReopenOutput reopens the audio output on the given device without touching
// the Spotify session, preserving the current playback position, paused state
// and volume. If playback is currently active it switches devices live (with a
// brief audio gap); if nothing is playing it just records the device for the
// next output open. It returns an error if the new device fails to open, in
// which case playback is left stopped.
func (p *Player) ReopenOutput(device string) error {
	resp := make(chan any, 1)
	if !p.send(playerCmd{typ: playerCmdReopenOutput, data: device, resp: resp}) {
		return ErrPlayerClosed
	}
	if err := <-resp; err != nil {
		return err.(error)
	}

	return nil
}

func (p *Player) SetPrimaryStream(source librespot.AudioSource, paused, drop bool) error {
	resp := make(chan any)
	if !p.send(playerCmd{typ: playerCmdSet, data: playerCmdDataSet{source: source, primary: true, paused: paused, drop: drop}, resp: resp}) {
		return ErrPlayerClosed
	}
	if err := <-resp; err != nil {
		return err.(error)
	}

	return nil
}

func (p *Player) SetSecondaryStream(source librespot.AudioSource) {
	resp := make(chan any)
	if !p.send(playerCmd{typ: playerCmdSet, data: playerCmdDataSet{source: source, primary: false}, resp: resp}) {
		return
	}
	<-resp
}

func (p *Player) httpChunkedReaderFromStorageResolve(log librespot.Logger, client *http.Client, storageResolve *downloadpb.StorageResolveResponse) (*audio.HttpChunkedReader, error) {
	if storageResolve.Result == downloadpb.StorageResolveResponse_STORAGE {
		return nil, fmt.Errorf("old storage not supported")
	} else if storageResolve.Result == downloadpb.StorageResolveResponse_RESTRICTED {
		return nil, fmt.Errorf("storage is restricted")
	} else if storageResolve.Result == downloadpb.StorageResolveResponse_CDN {
		if len(storageResolve.Cdnurl) == 0 {
			return nil, fmt.Errorf("no cdn urls")
		}

		log.Tracef("found %d cdn urls", len(storageResolve.Cdnurl))

		var err error
		for i := 0; i < len(storageResolve.Cdnurl); i++ {
			var cdnUrl *url.URL
			cdnUrl, err = url.Parse(storageResolve.Cdnurl[i])
			if err != nil {
				log.WithError(err).WithField("url", storageResolve.Cdnurl[i]).Warnf("failed parsing cdn url, trying next url")
				continue
			}

			if lastFailed, found := p.cdnQuarantine[cdnUrl.Host]; found {
				if i == len(storageResolve.Cdnurl)-1 {
					log.WithField("host", cdnUrl.Host).Warnf("cannot skip cdn url because it is the last one")
				} else if time.Since(lastFailed) < CdnUrlQuarantineDuration {
					log.WithField("host", cdnUrl.Host).Infof("skipping cdn url because it has failed recently")
					continue
				}
			}

			var rawStream *audio.HttpChunkedReader
			rawStream, err = audio.NewHttpChunkedReader(log, client, cdnUrl.String())
			if err != nil {
				log.WithError(err).WithField("host", cdnUrl.Host).Warnf("failed creating chunked reader, trying next url")
				p.cdnQuarantine[cdnUrl.Host] = time.Now()
				continue
			}

			delete(p.cdnQuarantine, cdnUrl.Host)
			return rawStream, nil
		}

		return nil, fmt.Errorf("failed creating chunked reader for any cdn url: %w", err)
	} else {
		return nil, fmt.Errorf("unknown storage resolve result: %s", storageResolve.Result)
	}
}

func (p *Player) retrieveAudioKey(ctx context.Context, spotId librespot.SpotifyId, fileId []byte) ([]byte, error) {
	if playplay.Plugin.IsSupported() {
		resp, err := p.sp.PlayPlayRequest(ctx, fileId, &streamingpb.PlayPlayLicenseRequest{
			Version:       ptr(playplay.Plugin.GetVersion()),
			Token:         playplay.Plugin.GetToken(),
			ContentType:   ptr(streamingpb.ContentType_AUDIO_TRACK),
			Interactivity: ptr(streamingpb.Interactivity_INTERACTIVE),
			Timestamp:     ptr(time.Now().Unix()),
		})
		if err != nil {
			return nil, fmt.Errorf("failed requesting playplay license: %w", err)
		}

		key, err := playplay.Plugin.Deobfuscate(resp.ObfuscatedKey, fileId)
		if err != nil {
			return nil, fmt.Errorf("failed deobfuscating playplay key: %w", err)
		}

		return key, nil
	}

	return p.audioKey.Request(ctx, spotId.Id(), fileId)
}

// Spotify normalizes to -14 dB LUFS according to ITU-R BS.1770 standard
// See https://support.spotify.com/us/artists/article/loudness-normalization/
const spotifyLoudnessTarget = -14.0

func calculateNormalisationFactor(params *audiofilespb.NormalizationParams, pregain float32) float32 {
	return normalisationFactorFor(params.LoudnessDb, params.TruePeakDb, pregain)
}

// normalisationFactorFor is the same calculation for audio that carries its
// loudness outside NormalizationParams, as DJ narration does in its
// narration.*.loudness and narration.*.true_peak metadata.
func normalisationFactorFor(loudnessDb, truePeakDb, pregain float32) float32 {
	// LoudnessDb is the integrated loudness of the track in LUFS (ITU-R BS.1770)
	// To normalize, calculate the gain needed to reach Spotify's target of -14 LUFS
	gainDb := spotifyLoudnessTarget - loudnessDb + pregain

	// Convert gain from dB to linear scale
	normalisationFactor := float32(math.Pow(10, float64(gainDb/20)))

	// TruePeakDb from audio files response is in dBTP (dB True Peak)
	truePeakLinear := float32(math.Pow(10, float64(truePeakDb)/20))
	if truePeakLinear <= 0 {
		return normalisationFactor
	}

	// Clamp to avoid exceeding full-scale (0 dBFS) after normalization
	// If the normalized peak would exceed 1.0, reduce the gain
	if normalisationFactor*truePeakLinear > 1 {
		normalisationFactor = 1 / truePeakLinear
	}
	return normalisationFactor
}

func (p *Player) getUnrestrictedTrack(ctx context.Context, spotId librespot.SpotifyId) (*metadatapb.Track, error) {
	var trackMeta metadatapb.Track
	err := p.sp.ExtendedMetadataSimple(ctx, spotId, extmetadatapb.ExtensionKind_TRACK_V4, &trackMeta)
	if err != nil {
		return nil, fmt.Errorf("failed getting track metadata: %w", err)
	}

	media := librespot.NewMediaFromTrack(&trackMeta)
	if !isMediaRestricted(media, *p.countryCode) {
		return &trackMeta, nil
	}

	for _, alt := range trackMeta.Alternative {
		media = librespot.NewMediaFromTrack(alt)
		if !isMediaRestricted(media, *p.countryCode) {
			// Clear alternatives to avoid confusion
			trackMeta.Alternative = nil

			// The alternative track does not have all fields set, copy them over
			// to the original track metadata.
			trackMeta.Gid = alt.Gid
			trackMeta.File = alt.File
			trackMeta.Preview = alt.Preview
			trackMeta.OriginalAudio = alt.OriginalAudio
			return &trackMeta, nil
		}
	}

	// We tried all alternatives, still restricted
	return nil, librespot.ErrMediaRestricted
}

// ResolvedAudio holds a decrypted but still encoded audio file for delivery
// over the API: the bytes after AES decryption, still in the original OGG
// Vorbis, FLAC or MP3 encoding. It stops short of the codec decode NewStream
// performs, so it can be streamed to a client unchanged.
type ResolvedAudio struct {
	io.ReadSeeker
	Size        int64
	ContentType string
	FormatName  string
	Media       *librespot.Media

	closeFn func() error
}

func (r *ResolvedAudio) Close() error {
	if r.closeFn == nil {
		return nil
	}
	return r.closeFn()
}

// ResolveDecryptedAudio fetches and decrypts the best audio file for a track or
// episode, returning the still-encoded bytes (OGG/FLAC/MP3) ready to stream.
// It mirrors the first half of NewStream — metadata, format selection, key
// retrieval, download and decryption — but stops before codec decoding.
func (p *Player) ResolveDecryptedAudio(ctx context.Context, client *http.Client, spotId librespot.SpotifyId, bitrate int) (*ResolvedAudio, error) {
	var media *librespot.Media
	var file *metadatapb.AudioFile
	if spotId.Type() == librespot.SpotifyIdTypeTrack {
		trackMeta, err := p.getUnrestrictedTrack(ctx, spotId)
		if err != nil {
			return nil, err
		}

		media = librespot.NewMediaFromTrack(trackMeta)
		spotId = media.Id()

		var audioFilesResp audiofilespb.AudioFilesExtensionResponse
		err = p.sp.ExtendedMetadataSimple(ctx, spotId, extmetadatapb.ExtensionKind_AUDIO_FILES, &audioFilesResp)
		if err != nil {
			return nil, fmt.Errorf("failed getting audio files metadata: %w", err)
		}

		var audioFiles []*metadatapb.AudioFile
		for _, f := range audioFilesResp.Files {
			audioFiles = append(audioFiles, f.File)
		}

		file = selectBestMediaFormat(audioFiles, bitrate, p.flacEnabled)
		if file == nil {
			return nil, librespot.ErrNoSupportedFormats
		}
	} else if spotId.Type() == librespot.SpotifyIdTypeEpisode {
		var episodeMeta metadatapb.Episode
		err := p.sp.ExtendedMetadataSimple(ctx, spotId, extmetadatapb.ExtensionKind_EPISODE_V4, &episodeMeta)
		if err != nil {
			return nil, fmt.Errorf("failed getting episode metadata: %w", err)
		}

		media = librespot.NewMediaFromEpisode(&episodeMeta)
		if isMediaRestricted(media, *p.countryCode) {
			return nil, librespot.ErrMediaRestricted
		}

		file = selectBestMediaFormat(episodeMeta.Audio, bitrate, p.flacEnabled)
		if file == nil {
			return nil, librespot.ErrNoSupportedFormats
		}
	} else {
		return nil, fmt.Errorf("unsupported spotify type: %s", spotId.Type())
	}

	audioKey, err := p.retrieveAudioKey(ctx, spotId, file.FileId)
	if err != nil {
		return nil, fmt.Errorf("failed retrieving audio key: %w", err)
	}

	// Prefer a cached copy of the encrypted audio file when available, mirroring
	// NewStream; the audio key is still required to decrypt it.
	var rawStream librespot.SizedReadAtSeeker
	if p.cache != nil {
		if cached, ok := p.cache.File(file.FileId); ok {
			rawStream = cached
		}
	}

	if rawStream == nil {
		storageResolve, err := p.sp.ResolveStorageInteractive(ctx, file.FileId, file.Format, false)
		if err != nil {
			return nil, fmt.Errorf("failed resolving track storage: %w", err)
		}

		httpStream, err := p.httpChunkedReaderFromStorageResolve(p.log, client, storageResolve)
		if err != nil {
			return nil, fmt.Errorf("failed creating chunked reader: %w", err)
		}

		// Persist the encrypted file to the cache once it has been fully
		// downloaded, best-effort like NewStream.
		if p.cache != nil {
			fileId := file.FileId
			httpStream.OnComplete(func(r io.ReaderAt, size int64) {
				if err := p.cache.SaveFile(fileId, io.NewSectionReader(r, 0, size)); err != nil {
					p.log.WithError(err).Warnf("failed caching audio file")
				}
			})
		}

		rawStream = httpStream
	}

	decryptedStream, err := audio.NewAesAudioDecryptor(rawStream, audioKey)
	if err != nil {
		return nil, fmt.Errorf("failed initializing audio decryptor: %w", err)
	}

	contentType, formatName := audioFormatContentType(*file.Format)

	// OGG Vorbis streams from Spotify carry a custom metadata page at the
	// start (seek table, replay gain) in place of the standard Vorbis
	// identification header. Strip it so downstream players see a valid
	// Ogg Vorbis bitstream. Also trim any trailing non-Ogg bytes (CDN
	// padding that remains after AES decryption), otherwise tools like
	// mutagen fail to re-save the file after tagging.
	var rdr io.ReadSeeker
	var size int64
	if GetAudioFileFormatAudioFormat(*file.Format) == AudioFormatOGGVorbis {
		audioStream, _, err := vorbis.ExtractMetadataPage(p.log, decryptedStream, rawStream.Size())
		if err != nil {
			return nil, fmt.Errorf("failed extracting Ogg metadata page: %w", err)
		}

		// audioStream spans [firstPageEnd, rawStream.Size()). Determine the
		// real end of the last Ogg page so trailing padding is excluded.
		firstPageEnd := rawStream.Size() - audioStream.Size()
		lastPageEnd := findLastOggPageEnd(decryptedStream, rawStream.Size())
		if lastPageEnd <= firstPageEnd {
			lastPageEnd = rawStream.Size()
		}

		contentSize := lastPageEnd - firstPageEnd
		rdr = io.NewSectionReader(decryptedStream, firstPageEnd, contentSize)
		size = contentSize
	} else {
		sr := io.NewSectionReader(decryptedStream, 0, rawStream.Size())
		rdr = sr
		size = sr.Size()
	}

	return &ResolvedAudio{
		ReadSeeker:  rdr,
		Size:        size,
		ContentType: contentType,
		FormatName:  formatName,
		Media:       media,
		closeFn:     decryptedStream.Close,
	}, nil
}

// findLastOggPageEnd reads the tail of r (up to totalSize) to locate the final
// Ogg page boundary. Returns the byte offset immediately after the last
// complete page, or 0 if no valid page was found. The window is sized above
// the maximum possible Ogg page size (255 lacing values of 255 bytes each
// plus header, ~65307 bytes) so the last page header always fits.
func findLastOggPageEnd(r io.ReaderAt, totalSize int64) int64 {
	const window = 66 * 1024
	off := max(0, totalSize-window)
	buf := make([]byte, totalSize-off)
	if _, err := r.ReadAt(buf, off); err != nil && err != io.EOF {
		return 0
	}

	// Search backwards for the "OggS" magic in the window.
	searchEnd := len(buf)
	for {
		idx := bytes.LastIndex(buf[:searchEnd], []byte("OggS"))
		if idx < 0 {
			return 0
		}
		pos := off + int64(idx)
		searchEnd = idx // keep searching if this page header is truncated

		// Read the full page header (27 bytes) from the position.
		hdr := make([]byte, 27)
		if _, err := r.ReadAt(hdr, pos); err != nil {
			continue
		}

		segments := int(hdr[26])
		pageSize := int64(27 + segments)
		if pageSize > totalSize-pos {
			// Page header says more segments than fit: truncated, try earlier.
			continue
		}

		// Read the segment table.
		segTable := make([]byte, segments)
		if _, err := r.ReadAt(segTable, pos+27); err != nil {
			continue
		}

		for _, s := range segTable {
			pageSize += int64(s)
		}

		if pageSize > totalSize-pos {
			continue
		}

		return pos + pageSize
	}
}

func audioFormatContentType(format metadatapb.AudioFile_Format) (contentType, formatName string) {
	formatName = format.String()
	switch GetAudioFileFormatAudioFormat(format) {
	case AudioFormatOGGVorbis:
		return "audio/ogg", formatName
	case AudioFormatFLAC:
		return "audio/flac", formatName
	case AudioFormatMP3:
		return "audio/mpeg", formatName
	default:
		return "application/octet-stream", formatName
	}
}

func (p *Player) NewStream(ctx context.Context, client *http.Client, spotId librespot.SpotifyId, bitrate int, mediaPosition int64) (*Stream, error) {
	log := p.log.WithField("uri", spotId.Uri())

	// Remember the id the caller asked for: spotId is reassigned below when a
	// restricted track is relinked to an alternative.
	requestedId := spotId

	playbackId := make([]byte, 16)
	_, _ = rand.Read(playbackId)

	p.events.PreStreamLoadNew(playbackId, spotId, mediaPosition)

	var normalisationFactor float32
	var media *librespot.Media
	var file *metadatapb.AudioFile
	if spotId.Type() == librespot.SpotifyIdTypeTrack {
		trackMeta, err := p.getUnrestrictedTrack(ctx, spotId)
		if err != nil {
			return nil, err
		}

		media = librespot.NewMediaFromTrack(trackMeta)
		spotId = media.Id()

		var audioFilesResp audiofilespb.AudioFilesExtensionResponse
		err = p.sp.ExtendedMetadataSimple(ctx, spotId, extmetadatapb.ExtensionKind_AUDIO_FILES, &audioFilesResp)
		if err != nil {
			return nil, fmt.Errorf("failed getting audio files metadata: %w", err)
		}

		var audioFiles []*metadatapb.AudioFile
		for _, f := range audioFilesResp.Files {
			audioFiles = append(audioFiles, f.File)
		}

		file = selectBestMediaFormat(audioFiles, bitrate, p.flacEnabled)
		if file == nil {
			return nil, librespot.ErrNoSupportedFormats
		}

		if p.normalisationEnabled {
			if p.normalisationUseAlbumGain {
				normalisationFactor = calculateNormalisationFactor(
					audioFilesResp.DefaultAlbumNormalizationParams,
					p.normalisationPregain,
				)
			} else {
				normalisationFactor = calculateNormalisationFactor(
					audioFilesResp.DefaultFileNormalizationParams,
					p.normalisationPregain,
				)
			}
		} else {
			normalisationFactor = 1
		}
	} else if spotId.Type() == librespot.SpotifyIdTypeEpisode {
		var episodeMeta metadatapb.Episode
		err := p.sp.ExtendedMetadataSimple(ctx, spotId, extmetadatapb.ExtensionKind_EPISODE_V4, &episodeMeta)
		if err != nil {
			return nil, fmt.Errorf("failed getting episode metadata: %w", err)
		}

		media = librespot.NewMediaFromEpisode(&episodeMeta)
		if isMediaRestricted(media, *p.countryCode) {
			return nil, librespot.ErrMediaRestricted
		}

		file = selectBestMediaFormat(episodeMeta.Audio, bitrate, p.flacEnabled)
		if file == nil {
			return nil, librespot.ErrNoSupportedFormats
		}

		normalisationFactor = 1
	} else {
		return nil, fmt.Errorf("unsupported spotify type: %s", spotId.Type())
	}

	p.events.PostStreamResolveAudioFile(playbackId, int32(bitrate), media, file)

	log.Debugf("selected format %s (%x)", file.Format.String(), file.FileId)

	audioKey, err := p.retrieveAudioKey(ctx, spotId, file.FileId)
	if err != nil {
		return nil, fmt.Errorf("failed retrieving audio key: %w", err)
	}

	p.events.PostStreamRequestAudioKey(playbackId)

	// Prefer a cached copy of the encrypted audio file when available: this
	// avoids resolving storage and downloading from the CDN entirely. The audio
	// key is still required to decrypt it below.
	var rawStream librespot.SizedReadAtSeeker

	// Close the raw stream if stream setup fails before it is handed off to a
	// Stream (e.g. a cached file that fails to decode); this releases the open
	// file handle or cancels the in-flight download.
	streamHandedOff := false
	defer func() {
		if !streamHandedOff {
			if closer, ok := rawStream.(io.Closer); ok && closer != nil {
				_ = closer.Close()
			}
		}
	}()

	if p.cache != nil {
		if cached, ok := p.cache.File(file.FileId); ok {
			log.Debugf("using cached audio file (%d bytes)", cached.Size())
			rawStream = cached
		}
	}

	if rawStream == nil {
		storageResolve, err := p.sp.ResolveStorageInteractive(ctx, file.FileId, file.Format, false)
		if err != nil {
			return nil, fmt.Errorf("failed resolving track storage: %w", err)
		}

		p.events.PostStreamResolveStorage(playbackId)

		httpStream, err := p.httpChunkedReaderFromStorageResolve(log, client, storageResolve)
		if err != nil {
			return nil, fmt.Errorf("failed creating chunked reader: %w", err)
		}

		p.events.PostStreamInitHttpChunkReader(playbackId, httpStream)

		// Persist the encrypted file to the cache once it has been fully
		// downloaded. This is best-effort: caching failures never affect
		// playback.
		if p.cache != nil {
			fileId := file.FileId
			httpStream.OnComplete(func(r io.ReaderAt, size int64) {
				if err := p.cache.SaveFile(fileId, io.NewSectionReader(r, 0, size)); err != nil {
					log.WithError(err).Warnf("failed caching audio file")
				}
			})
		}

		rawStream = httpStream
	}

	decryptedStream, err := audio.NewAesAudioDecryptor(rawStream, audioKey)
	if err != nil {
		return nil, fmt.Errorf("failed intializing audio decryptor: %w", err)
	}

	var stream librespot.AudioSource
	var sampleRate, bitDepth int32

	audioFormat := GetAudioFileFormatAudioFormat(*file.Format)
	if audioFormat == AudioFormatOGGVorbis {
		audioStream, meta, err := vorbis.ExtractMetadataPage(p.log, decryptedStream, rawStream.Size())
		if err != nil {
			return nil, fmt.Errorf("failed reading metadata page: %w", err)
		}

		vorbisStream, err := vorbis.New(log, audioStream, meta, normalisationFactor)
		if err != nil {
			return nil, fmt.Errorf("failed initializing ogg vorbis stream: %w", err)
		}

		if vorbisStream.SampleRate != SampleRate {
			return nil, fmt.Errorf("unsupported sample rate: %d", vorbisStream.SampleRate)
		} else if vorbisStream.Channels != Channels {
			return nil, fmt.Errorf("unsupported channels: %d", vorbisStream.Channels)
		}

		stream = vorbisStream
		sampleRate = vorbisStream.SampleRate
	} else if audioFormat == AudioFormatFLAC {
		audioStream := io.NewSectionReader(decryptedStream, 0, rawStream.Size())
		flacStream, err := flac.New(log, audioStream, normalisationFactor)
		if err != nil {
			return nil, fmt.Errorf("failed initializing flac stream: %w", err)
		}

		if flacStream.SampleRate != SampleRate {
			return nil, fmt.Errorf("unsupported sample rate: %d", flacStream.SampleRate)
		} else if flacStream.Channels != Channels {
			return nil, fmt.Errorf("unsupported channels: %d", flacStream.Channels)
		}

		stream = flacStream
		sampleRate = flacStream.SampleRate
		bitDepth = flacStream.BitDepth
	} else if audioFormat == AudioFormatMP3 {
		audioStream := io.NewSectionReader(decryptedStream, 0, rawStream.Size())
		mp3Stream, err := mp3.New(log, audioStream, normalisationFactor)
		if err != nil {
			return nil, fmt.Errorf("failed initializing mp3 stream: %w", err)
		}

		if mp3Stream.SampleRate != SampleRate {
			return nil, fmt.Errorf("unsupported sample rate: %d", mp3Stream.SampleRate)
		} else if mp3Stream.Channels != Channels {
			return nil, fmt.Errorf("unsupported channels: %d", mp3Stream.Channels)
		}

		stream = mp3Stream
		sampleRate = mp3Stream.SampleRate
	} else {
		return nil, fmt.Errorf("unsupported audio format: %s", *file.Format)
	}

	// Seek to the correct position if needed.
	if mediaPosition > 0 {
		if err := stream.SetPositionMs(max(0, min(mediaPosition, int64(media.Duration())))); err != nil {
			return nil, fmt.Errorf("failed seeking stream: %w", err)
		}
	}

	streamHandedOff = true
	return &Stream{
		PlaybackId:  playbackId,
		RequestedId: requestedId,
		Source:      stream,
		Media:       media,
		File:        file,
		SampleRate:  sampleRate,
		BitDepth:    bitDepth,
	}, nil
}
