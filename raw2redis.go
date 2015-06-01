/**
 * find out processed file list from [table name] folder journal.txt
 * find out all files from [table name] folder
 * diff new files
 * foreach new files
 * tar file to a temp folder
 * check file type
 * run cmd to convert file into csv
 * load csv to redis use go script
 * 增加日志信息在每个处理过的文件上一行
 * write filename into file
 * clean tempPath
 * raw2redis
 * sudo docker run --rm -it -v "$PWD":/usr/src/myapp -w /usr/src/myapp golang go build -o raw2redis
 * sudo docker run --rm -it -v "$PWD":/usr/src/myapp -w /usr/src/myapp golang go build -o csv-loader
 * /home/github/raw2redis/raw2redis -table="IMP_URUGUAY" -table-path="/home/github/customs-sync/IMP_URUGUAY" -cmd='/home/github/csv-loader/csv-loader -redis-addr="192.168.11.100:6379" set-IMP_URUGUAY-oracle'
 */

package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

var (
	table     string
	tablePath string
	tempPath  string
	cmd       string
)

const journalFileName = "journal.txt"

func init() {
	flag.StringVar(&table, "table", "", "the customs data table name")
	flag.StringVar(&tablePath, "table-path", "", "the customs data folder of this country")
	flag.StringVar(&cmd, "cmd", "", "the csv-loader command")
	flag.Parse()

	if flag.NArg() < 0 || table == "" || tablePath == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	log.Println(cmd)

	tempPath = "/tmp/" + table
}

func main() {
	journalPath := tablePath + "/" + journalFileName

	journal := readJournal(journalPath)

	lines := excludeCommentLines(journal)

	dirFiles := readDirFiles(tablePath, journalFileName)

	newFiles := diffNewFiles(lines, dirFiles)

	journal = append(journal, "# "+time.Now().Format("2006-01-02 15:04:05"))

	for _, f := range newFiles {
		extractFile(tablePath+"/"+f, tempPath)
		extractedFiles := readDirFiles(tempPath, "")

		for _, file := range extractedFiles {
			path := tempPath + "/" + file
			log.Print(path)

			// convert to csv
			ext := filepath.Ext(path)
			var convertCmd string
			switch ext {
			case ".mdb":
				fallthrough
			case ".accdb":
				convertCmd = "mdb-export " + path + " $(mdb-tables " + path + ") | " + cmd
			case ".xls":
				fallthrough
			case ".xlsx":
				convertCmd = "ssconvert --export-type=Gnumeric_stf:stf_csv " + path + " fd://1 | " + cmd
			case ".txt":
				// remove file
        err := os.Remove(path)
				if err != nil {
					log.Fatalf("remove file %s error: %s", path, err)
				}
				continue
			default:
				log.Printf("error: unknow file type %s", ext)
			}
			cmd := exec.Command("bash", "-c", convertCmd)
			var out bytes.Buffer
			cmd.Stdout = &out
			err := cmd.Run()
			if err != nil {
				log.Fatalf("exec %s error: %s out: %s", convertCmd, err, out.String())
			}

			// remove file
			err = os.Remove(path)
			if err != nil {
				log.Fatalf("remove file %s error: %s", path, err)
			}
		}

		journal = append(journal, f)
		// write journal
		writeLines(journal, journalPath)
	}

	log.Printf("Mission complete %v", newFiles)
}

// ---------------------------------------------------------------------------------------------- //

func extractFile(path string, tempPath string) {
	ext := filepath.Ext(path)
	var extractCmd string
	if ext == ".rar" {
		extractCmd = "unrar e -o+ " + path + " " + tempPath
	} else if ext == ".tgz" || ext == ".zip" {
		extractCmd = "tar xzvf --overwrite " + path + " -C " + tempPath
	}
	// mkdir
	if _, err := os.Stat(tempPath); os.IsNotExist(err) {
		log.Printf("create tempPath directory: %s", tempPath)
		err = os.Mkdir(tempPath, os.FileMode(int(0777)))
		if err != nil {
			log.Fatalf("create directory error: %s", err)
		}
	}

	log.Print(extractCmd)
	cmd := exec.Command("bash", "-c", extractCmd)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Fatalf("exec %s error: %s out: %s", extractCmd, err, out.String())
	}
}

func readJournal(path string) []string {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Printf("no such file or directory: %s", path)
		return make([]string, 0)
	}
	return readLines(path)
}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) []string {
	file, err := os.Open(path)
	if err != nil {
		file.Close()
		log.Fatalf("readLines error: %s", err)
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if scanner.Err() != nil {
		log.Fatalf("readLines error: %s", err)
	}
	return lines
}

func excludeCommentLines(lines []string) []string {
	var result []string
	for _, line := range lines {
		// ignore comment line
		if !strings.HasPrefix(line, "#") {
			result = append(result, line)
		}
	}
	return result
}

// writeLines writes the lines to the given file.
func writeLines(lines []string, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
	return w.Flush()
}

func readDirFiles(dirname string, excludeFile string) []string {
	var result []string
	files, _ := ioutil.ReadDir(dirname)
	for _, file := range files {
		if !file.IsDir() && (file.Name() != excludeFile) {
			result = append(result, file.Name())
		}
	}
	return result
}

func diffNewFiles(processedFileList []string, dirFiles []string) []string {
	var result []string
	result = compare(dirFiles, processedFileList)
	return result
}

func compare(X, Y []string) []string {
	m := make(map[string]int)

	for _, y := range Y {
		m[y]++
	}

	var ret []string
	for _, x := range X {
		if m[x] > 0 {
			m[x]--
			continue
		}
		ret = append(ret, x)
	}

	return ret
}
