package main

import (
	"flag"
	"fmt"
	"strings"
	"golang.org/x/net/context"
	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/logadmin"
	"google.golang.org/api/dataflow/v1b3"
	"google.golang.org/api/iterator"
	"log"
	"time"
	"os"
	"bufio"
	"runtime"
	"encoding/json"
	"gopkg.in/gomail.v2"
	"strconv"
	"golang.org/x/oauth2/google"
)

/*
go run logmine.go \
-logFilter='resource.type="container" resource.labels.cluster_name="qa-trinity" severity>=WARNING' \
-outputDir=./output \
-source=qa_trinity \
-maxEntryCount=20 \
-rangeInMinutes=10 \
-outputFile=qa_trinity_output.log

go run logmine.go \
-logFilter='resource.type=dataflow_step resource.labels.job_id=DATAFLOW_JOB_NAME' \
-outputDir=./output \
-source=qa-trinity-event-store \
-maxEntryCount=20 \
-rangeInMinutes=10 \
-outputFile=qa_trinity_event_store_output.log
 */

type StopFile struct {
	entry  string
	count  []int
}

type StopFiles []StopFile

// define a struct to pass around args since we don't have classes with member fields or functions
type logmine struct {
	filter  string
	minutes	int
	source  string
	project string
	count   int
	dir     string
	fname   string
	user    string
	password  string
}

func main() {
	logFilter := flag.String("logFilter", "resource.type=\"container\" resource.labels.cluster_name=\"qa-trinity\" severity>=WARNING", "log query filter")
	maxEntryCount := flag.Int("maxEntryCount", 20, "max per log entry to save to file")
	outputDir := flag.String("outputDir", "/tmp", "directory to store all files")
	outputFile := flag.String("outputFile", "/output.log", "output file name")
	rangeInMinutes := flag.Int("rangeInMinutes", 0, "past number if minutes to query")
	sender := flag.String("sender", "no-reply@nestlabs.com", "email sender")
	recipients := flag.String("recipients", "tlichtenberg@google.com", "email recipients")  // TODO figure out slice flags or comma-split this
	source := flag.String("source", "", "log source")
	project := flag.String("project", "nest-ds-dev", "gcp project")
	user := flag.String("user", "", "gcp project")
	password := flag.String("password", "", "gcp project")

	// got to flag.Parse() else defaults remain as values
	flag.Parse()

	// initialize the logmine struct with arg values
	mine := logmine{
					   filter: *logFilter,
					   minutes: *rangeInMinutes,
					   source: *source,
					   project: *project,
					   count: *maxEntryCount,
					   dir: *outputDir,
					   fname: *outputFile,
					   user: *user,
					   password: *password,
				   }

	// got to work with flag values AFTER parse
	recipientsList := strings.Split(*recipients, ",")
	fmt.Printf("sender = %v\n", *sender)
	// iterate through the slice using range
	for i := range recipientsList {
		fmt.Println(recipientsList[i])
	}

	ctx := context.Background()
	//dataflowService, err := dataflow.New(oauthHttpClient)
	adminClient, err := logadmin.NewClient(ctx, *project)
	if err != nil {
		log.Fatalf("Failed to create logadmin client: %v", err)
	}

	printMine(mine)
	entries, err := doQuery(adminClient, mine)
	newData, _ := doProcess(mine, entries)
	doSendMail(mine, *sender, recipientsList, newData)
}

func printMine(mine logmine) {
	fmt.Printf("maxEntryCount = %v\n", mine.count)
	fmt.Printf("outputDir = %v\n", mine.dir)
	fmt.Printf("outputFile = %v\n", mine.fname)
	fmt.Printf("rangeInMinutes = %v\n", mine.minutes)
	fmt.Printf("source = %v\n", mine.source)
	fmt.Printf("project = %v\n", mine.project)
	fmt.Printf("filter = %v\n", mine.filter)
}

func doQuery(adminClient *logadmin.Client, mine logmine) ([]*logging.Entry, error)  {

	var entries []*logging.Entry
	ctx := context.Background()

	// process filter
	filter := doFilterRevisions(mine)

	// [START list_log_entries]
	const name = "history"
	iter := adminClient.Entries(ctx,
		// Only get entries from the log-example log.
		logadmin.Filter(fmt.Sprintf("%s", filter)),
		// Get most recent entries first.
		logadmin.NewestFirst(),
	)

	// fetch the most recent maxEntryCount entries.
	for len(entries) < mine.count {
		entry, err := iter.Next()
		if err == iterator.Done {
			return entries, nil
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	log.Printf("Found %d entries.", len(entries))
	for _, entry := range entries {
		fmt.Printf("Entry: %6s @%s: %v\n\n",
			entry.Severity,
			entry.Timestamp.Format(time.RFC3339),
			entry.Payload)
	}
	// [END list_log_entries]

	return entries, nil
}

func doProcess(mine logmine, entries []*logging.Entry) ([]StopFile, []StopFile) {
	var newData = []StopFile{}
	var dataCounts = make(map[string] int)

	// read in existing stopfile, if any
	stopfile, err := readStopfile(mine)

	// read in the filter file data, if any
	filterFileData, _ := readFilterfile(mine)

	// count up all occurrences of each entry in the dataCounts map
	for i := range entries {
		payload := fmt.Sprintf("%s", entries[i].Payload)  // convert payload to a string
		//fmt.Printf("doProcess.payload: %s\n", payload)
		entry := doFilter(mine, payload, filterFileData)  // match against filter file data
		//fmt.Printf("doProcess.entry: %s\n\n", entry)
		if _, ok := dataCounts[entry]; ok {
			//fmt.Printf("incrementing dataCount for %s\n", entry)
			dataCounts[entry] += 1
		} else {
			//fmt.Printf("initializing dataCount for %s\n", entry)
			dataCounts[entry] = 1
		}
	}

	// if the dataCounts entry is in the stopfile, append its count
	// else make a new key in the stopfile for the entry with its count
	for k,v := range dataCounts {
		//fmt.Printf("k = %s, v = %d\n", k, v)
		var foundEntry = false
		for i := range stopfile {
			//fmt.Printf("stopfile entry: %s\n", stopfile[i].entry)
			a := strconv.QuoteToASCII(stopfile[i].entry)
			b := strconv.QuoteToASCII(k)
			if strings.Contains(a, b) {
				stopfile[i].count = append(stopfile[i].count, v)
				if len(stopfile[i].count) > mine.count {
					stopfile[i].count = stopfile[i].count[len(stopfile[i].count)-mine.count:]
				}
				foundEntry = true
				break
				}
		}
		if foundEntry == false {
			var c = make([]int,0)
			c = append(c, v)
			stopfile = append(stopfile, StopFile{entry: k, count: c})
			newData = append(newData, StopFile{entry: k, count: c})
		}
	}

	fmt.Printf("stopfile: %v\n\n", stopfile)
	err = writeStopfile(mine, stopfile)
	Check(err)

	fmt.Printf("newData: %v\n\n", newData)
	err = writeNewfile(mine, newData)
	Check(err)

	return newData, stopfile
}

func doFilter(mine logmine, logEntry string, filterFileData []string) string {
	for _, line :=  range filterFileData {
		line = strings.TrimSuffix(line, "\n")
		if strings.Contains(logEntry, line) {
			fmt.Printf("found filter line: %s\n", line)
			logEntry = line
			break
		}
	}
	return logEntry
}

func doFilterRevisions(mine logmine) string {
	newFilter := mine.filter
	if strings.Contains(mine.filter, "DATAFLOW_JOB_NAME") {
		newFilter = strings.Replace(mine.filter, "DATAFLOW_JOB_NAME", getDataflowJobByName(mine), 1)
	}

	if mine.minutes > 0 { // TODO verify this works. saw weird stuff on https://play.golang.org/
		minutes := time.Duration(mine.minutes)
		currentTime := time.Now()
		//fmt.Println("Current Time in String: ", currentTime.String())
		after := currentTime.Add(-minutes*time.Minute)
		newAfter := fmt.Sprintf(after.Format(time.RFC3339))
		newFilter = fmt.Sprintf("%s timestamp > \"%v\"", newFilter, newAfter)
		fmt.Println(newFilter)
	}

	return newFilter
}

func getDataflowJobByName(mine logmine) string {
	var jobId = "DATAFLOW_JOB_NOT_FOUND"
	ctx := context.Background()
	oauthHttpClient, err := google.DefaultClient(ctx,
		"https://www.googleapis.com/auth/devstorage.full_control")
	if err != nil {
		log.Fatal(err)
	}
	dataflowService, err := dataflow.New(oauthHttpClient)
	f := dataflowService.Projects.Jobs.List(mine.project)
	f.Filter("ACTIVE")
	jobs, err := f.Do()
	for _, job := range jobs.Jobs {
		//fmt.Printf("Name: %s, Id: %s\n", job.Name, job.Id)
		if strings.Contains(job.Name, mine.source) {
			jobId = job.Id
			fmt.Printf("found dataflow job name: %s, returning job id: %s\n", job.Name, job.Id)
			break
		}
	}
	return jobId
}

func writeTextfile(outfile string, stopfiles []StopFile) error {
	f, err := os.Create(outfile)
	Check(err)
	defer f.Close()
	for _, v := range stopfiles {
		s := fmt.Sprintf("%s:%v\n", v.entry, v.count)
		_, err = f.WriteString(s)
		f.Sync()
	}
	return err
}

func readTextfile(infile string) ([]string, error) {
	var lines []string
	file, err := os.Open(infile)
	if err != nil {
		return lines, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func writeStopfile(mine logmine, stopfile []StopFile) error {
	outfile := fmt.Sprintf("%s/stopfile_%s.txt", mine.dir, mine.source)
	err := writeTextfile(outfile, stopfile)
	return err
}

func readStopfile(mine logmine) ([]StopFile, error) {
	infile := fmt.Sprintf("%s/stopfile_%s.txt", mine.dir, mine.source)
	stopfiles := []StopFile{}
	lines, err := readTextfile(infile)
	for i :=  range lines {
		line := strings.Split(lines[i],":")
		var ints []int
		err = json.Unmarshal([]byte(line[1]), &ints)
		stop := StopFile{entry: line[0], count: ints}
		stopfiles = append(stopfiles, stop)
	}
	return stopfiles, err
}

func readFilterfile(mine logmine) ([]string, error) {
	infile := fmt.Sprintf("%s/filterfile_%s.txt", mine.dir, mine.source)
	lines, err := readTextfile(infile)
	return lines, err
}

func writeNewfile(mine logmine, newfile []StopFile) error {
	outfile := fmt.Sprintf("%s/newfile_%s.txt", mine.dir, mine.source)
	err := writeTextfile(outfile, newfile)
	return err
}

func Check(e error) {
	if e != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Println(line, "\t", file, "\n", e)
		os.Exit(1)
	}
}

func doSendMail(mine logmine, sender string, recipients []string,  data []StopFile)  {
	//auth := smtp.PlainAuth("", mine.user, mine.password, "localhost")
	htmlData := "<body><table border=2><tr><td> New Entries </td><td> Entry Count </td></tr>"
	for _, v := range data {
		htmlDataPart := fmt.Sprintf("<tr><td>%s</td><td>%v</td></tr>", v.entry, v.count)
		htmlData += htmlDataPart
	}
	htmlData += "</table></body>"
	//mime := "MIME-version: 1.0;\nContent-Type: text/html; charset=\"UTF-8\";\n\n";
	subj := fmt.Sprintf("(%s) Log Mine [Go]: %s - %s", mine.project, mine.source, time.Now())
	// msg := []byte(subj + mime + htmlData)

	for _, recipient := range recipients {
		m := gomail.NewMessage()
		m.SetHeader("From", sender)
		m.SetHeader("To", recipient)
		m.SetHeader("Subject", subj)
		m.SetBody("text/html", htmlData)

		// Send the email
		d := gomail.NewPlainDialer("localhost", 25, mine.user, mine.password)
		if err := d.DialAndSend(m); err != nil {
			fmt.Println(err)
		}
	}
	/*
	err := smtp.SendMail("localhost:25", auth, sender, recipient, msg)
	if err != nil {
		fmt.Printf("sendmail error: %s", err)
	}
	*/
}