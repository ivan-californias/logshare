package main

import (
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	gcs "cloud.google.com/go/storage"
	cloudflare "github.com/cloudflare/cloudflare-go"
	"github.com/cloudflare/logshare"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
	"golang.org/x/net/context"
)

// Rev is set on build time and should contain the git commit logshare-cli
// was built from.
var Rev = ""

func main() {
	log.SetPrefix("[logshare-cli] ")
	log.SetFlags(log.Ltime)
	log.SetOutput(os.Stderr)

	conf := &config{}
	app := cli.NewApp()
	app.Name = "logshare-cli"
	app.Usage = "Fetch request logs from Cloudflare's Enterprise Log Share API"
	app.Flags = flags
	app.Version = Rev
	app.Commands = []cli.Command {
		{
			Name: "loop",
			Usage: "Fetch logs in loop mode",
			Flags: loopFlags,
			Action: runLoop(conf),
		},
	}
	app.Action = run(conf)
	if err := app.Run(os.Args); err != nil {
		log.Println(err)
	}
}

func setupGoogleStr(projectID string, bucketName string, filename string) (*gcs.Writer, error) {
	gCtx := context.Background()

	gClient, error := gcs.NewClient(gCtx)
	if error != nil {
		return nil, error
	}

	gBucket := gClient.Bucket(bucketName)

	if error = gBucket.Create(gCtx, projectID, nil); strings.Contains(error.Error(), "409") {
		log.Printf("Bucket %v already exists.\n", bucketName)
		error = nil
	} else if error != nil {
		return nil, error
	}

	obj := gBucket.Object(filename)
	return obj.NewWriter(gCtx), error
}

func run(conf *config) func(c *cli.Context) error {
	return func(c *cli.Context) error {
		client, closers, err := GetClientFromConfigAndContext(conf, c)
		defer func () {
			for _, c := range closers {
				c.Close()
			}
		}()
		if err != nil {
			return err
		}
		return ClientWork(conf, client)
	}
}

func runLoop(conf *config) func(c *cli.Context) error {
	return func(c *cli.Context) error {
		loopTime := c.Int("loop-wait")
	//	checkPoint := c.String("checkpoint")

		client, closers, err := GetClientFromConfigAndContext(conf, c)
		defer func () {
			for _, c := range closers {
				c.Close()
			}
		}()
		if err != nil {
			return err
		}
		for {
			err = ClientWork(conf, client)
			if err != nil {
				log.Println(err)
			}
			log.Println("sleeping for seconds :", loopTime) 
			time.Sleep(time.Duration(loopTime) * time.Second)
		}
	}
}

func GetClientFromConfigAndContext(conf *config, c *cli.Context) (*logshare.Client, []io.Closer, error) {
	closers := make([]io.Closer, 0)
	if err := parseFlags(conf, c); err != nil {
		cli.ShowAppHelp(c)
		return nil, closers, err
	}

	// Populate the zoneID if it wasn't supplied.
	if conf.zoneID == "" {
		cf, err := cloudflare.New(conf.apiKey, conf.apiEmail)
		id, err := cf.ZoneIDByName(conf.zoneName)
		if err != nil {
			cli.ShowAppHelp(c)
			return nil, closers, errors.Wrap(err, "could not find a zone for the given ID")
		}

		conf.zoneID = id
	}

	var outputWriter io.Writer
	if conf.googleStorageBucket != "" {
		fileName := "cloudflare_els_" + conf.zoneID + "_" + strconv.Itoa(int(time.Now().Unix())) + ".json"

		gcsWriter, err := setupGoogleStr(conf.googleProjectID, conf.googleStorageBucket, fileName)
		if err != nil {
			return nil, closers, err
		}
		closers = append(closers, gcsWriter)
		outputWriter = gcsWriter
	}

	client, err := logshare.New(
		conf.apiKey,
		conf.apiEmail,
		&logshare.Options{
			Fields:          conf.fields,
			Dest:            outputWriter,
			ByReceived:      true,
			Sample:          conf.sample,
			TimestampFormat: conf.timestampFormat,
		})
	if err != nil {
		return nil, closers, err
	}
	return client, closers, nil
}

func ClientWork(conf *config, client *logshare.Client) error {
	// Based on the combination of flags, call against the correct log
	// endpoint.
	var meta *logshare.Meta
	var err error

	if conf.listFields {
		meta, err = client.FetchFieldNames(conf.zoneID)
		if err != nil {
			return errors.Wrap(err, "failed to fetch field names")
		}
	} else {
		meta, err = client.GetFromTimestamp(
			conf.zoneID, conf.startTime, conf.endTime, conf.count)
		if err != nil {
			return errors.Wrap(err, "failed to fetch via timestamp")
		}
	}

	log.Printf("HTTP status %d | %dms | %s",
		meta.StatusCode, meta.Duration, meta.URL)
	log.Printf("Retrieved %d logs", meta.Count)

	return nil
}

func parseFlags(conf *config, c *cli.Context) error {
	conf.apiKey = c.GlobalString("api-key")
	conf.apiEmail = c.GlobalString("api-email")
	conf.zoneID = c.GlobalString("zone-id")
	conf.zoneName = c.GlobalString("zone-name")
	conf.startTime = c.GlobalInt64("start-time")
	conf.endTime = c.GlobalInt64("end-time")
	conf.count = c.GlobalInt("count")
	conf.timestampFormat = c.GlobalString("timestamp-format")
	conf.sample = c.GlobalFloat64("sample")
	conf.fields = c.GlobalStringSlice("fields")
	conf.listFields = c.GlobalBool("list-fields")
	conf.googleStorageBucket = c.GlobalString("google-storage-bucket")
	conf.googleProjectID = c.GlobalString("google-project-id")

	return conf.Validate()
}

type config struct {
	apiKey              string
	apiEmail            string
	zoneID              string
	zoneName            string
	startTime           int64
	endTime             int64
	count               int
	timestampFormat     string
	sample              float64
	fields              []string
	listFields          bool
	googleStorageBucket string
	googleProjectID     string
}

func (conf *config) Validate() error {

	if conf.apiKey == "" || conf.apiEmail == "" {
		return errors.New("Must provide both api-key and api-email")
	}

	if conf.zoneID == "" && conf.zoneName == "" {
		return errors.New("zone-name OR zone-id must be set")
	}

	if conf.sample != 0.0 && (conf.sample < 0.1 || conf.sample > 0.9) {
		return errors.New("sample must be between 0.1 and 0.9")
	}

	if (conf.googleStorageBucket == "") != (conf.googleProjectID == "") {
		return errors.New("Both google-storage-bucket and google-project-id must be provided to upload to Google Storage")
	}

	return nil
}

var flags = []cli.Flag{
	cli.StringFlag{
		Name:  "api-key",
		Usage: "Your Cloudflare API key",
	},
	cli.StringFlag{
		Name:  "api-email",
		Usage: "The email address associated with your Cloudflare API key and account",
	},
	cli.StringFlag{
		Name:  "zone-id",
		Usage: "The zone ID of the zone you are requesting logs for",
	},
	cli.StringFlag{
		Name:  "zone-name",
		Usage: "The name of the zone you are requesting logs for. logshare will automatically fetch the ID of this zone from the Cloudflare API",
	},
	cli.StringFlag{
		Name:  "ray-id",
		Usage: "The ray ID to request logs from (instead of a timestamp)",
	},
	cli.Int64Flag{
		Name:  "start-time",
		Value: time.Now().Add(-time.Minute * 30).Unix(),
		Usage: "The timestamp (in Unix seconds) to request logs from. Defaults to 30 minutes behind the current time",
	},
	cli.Int64Flag{
		Name:  "end-time",
		Value: time.Now().Add(-time.Minute * 20).Unix(),
		Usage: "The timestamp (in Unix seconds) to request logs to. Defaults to 20 minutes behind the current time",
	},
	cli.IntFlag{
		Name:  "count",
		Value: 1,
		Usage: "The number (count) of logs to retrieve. Pass '-1' to retrieve all logs for the given time period",
	},
	cli.Float64Flag{
		Name:  "sample",
		Value: 0.0,
		Usage: "The sampling rate from 0.1 (10%) to 0.9 (90%) to use when retrieving logs",
	},
	cli.StringFlag{
		Name:  "timestamp-format",
		Value: "unixnano",
		Usage: "The timestamp format to use in logs: one of 'unix', 'unixnano', or 'rfc3339'",
	},
	cli.StringSliceFlag{
		Name:  "fields",
		Usage: "Select specific fields to retrieve in the log response. Pass a comma-separated list to fields to specify multiple fields.",
	},
	cli.BoolFlag{
		Name:  "list-fields",
		Usage: "List the available log fields for use with the --fields flag",
	},
	cli.StringFlag{
		Name:  "google-storage-bucket",
		Usage: "Full URI to a Google Cloud Storage Bucket to upload logs to",
	},
	cli.StringFlag{
		Name:  "google-project-id",
		Usage: "Project ID of the Google Cloud Storage Bucket to upload logs to",
	},
}

var loopFlags = []cli.Flag{
	cli.IntFlag{
		Name:  "loop-wait",
		Value: 60,
		Usage: "The number seconds to wait after every loop cycle",
	},
	cli.StringFlag{
		Name:  "checkpoint",
		Value: "timestamp",
		Usage: "The type of checkpoint to use: 'timestamp' or 'ray-id'",
	},
}
