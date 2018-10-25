package changeset_test

import (
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/omniscale/go-osm/replication/changeset"
)

func Example() {
	// This example shows how to automatically download OSM changeset files.

	// We store all changesets in a temporary directory for this example.
	changesetDir, err := ioutil.TempDir("", "")
	if err != nil {
		log.Fatal("unable to create changeset dir:", err)
	}
	defer os.RemoveAll(changesetDir)

	// Where do we fetch our changesets from?
	url := "https://planet.openstreetmap.org/replication/changesets/"

	// Query the ID of the latest changeset.
	seqID, err := changeset.CurrentSequence(url)
	if err != nil {
		log.Fatal("unable to fetch current sequence:", err)
	}

	// Start downloader to fetch changesets. Start with a previous sequence ID, so
	// that we don't have to wait till the files are available.
	dl := changeset.NewDownloader(changesetDir, url, seqID-5, time.Minute)

	// Iterate all changesets as they are downloaded
	downloaded := 0
	for seq := range dl.Sequences() {
		downloaded++

		// seq contains the Filename of the downloaded changeset file. You can use parser/changeset to parse the content.
		log.Printf("downloaded changeset #%d with changes till %s to %s", seq.Sequence, seq.Time, seq.Filename)

		if downloaded == 3 {
			// Stop downloading after 3 changesets for this example.
			// (Stop() closes the channel from dl.Sequences and for our loop will stop).
			dl.Stop()
		}
	}
	// Output:
}
