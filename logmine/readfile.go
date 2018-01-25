package main

import (
	"flag"
	"fmt"
	"os"
	"bufio"
)

/*
go run readfile.go \
-outputDir=./output \
-outputFile=stopfile_qa_trinity.txt
 */


func main() {
	outputDir := flag.String("outputDir", "/tmp", "directory to store all files")
	outputFile := flag.String("outputFile", "/output.log", "output file name")

	// got to flag.Parse() else defaults remain as values
	flag.Parse()

	// initialize the logmine struct with arg values
	infile := fmt.Sprintf("%s/%s", *outputDir, *outputFile)
	fmt.Printf("reading from %s\n\n", infile)
	lines, _ := readLines(infile)
	fmt.Println(lines)
	for _, v := range lines {
		fmt.Println(v)
	}

}

func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func readThefile(infile string) ([]string, error) {
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