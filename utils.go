package main

import (
	"crypto/rand"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"path"
	"runtime"
	"strconv"
)

func SetLogger(level string, serverID int, production bool) {
	// Parse log level
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		panic(fmt.Sprintf("Invalid log level: %v", err))
	}
	log.SetLevel(lvl)

	// Create server-specific log folder
	logDir := fmt.Sprintf("./logs/server%d", serverID)
	if err := os.MkdirAll(logDir, os.ModePerm); err != nil {
		panic(fmt.Sprintf("Failed to create log folder: %v", err))
	}
	log.Infof("Log folder: %s", logDir)
	logFile := path.Join(logDir, fmt.Sprintf("server%d.txt", serverID))

	// Open with O_TRUNC to overwrite the previous log instead of appending
	file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Println("Failed to log to file, using default stderr")
	} else {
		log.Out = file
	}

	// Use TextFormatter with full timestamp
	log.Formatter = &logrus.TextFormatter{
		FullTimestamp:   true,
		DisableColors:   false,
		TimestampFormat: "2006-01-02 15:04:05",
	}

	// Optional: include caller info for easier debugging
	if !production {
		log.SetReportCaller(true)
		log.SetFormatter(&logrus.TextFormatter{
			CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
				fileName := path.Base(frame.File) + ":" + strconv.Itoa(frame.Line)
				return "", fileName + " >>"
			},
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02 15:04:05",
		})
	}
}

func genRandomBytes(length int) []byte {
	randomBytes := make([]byte, length)
	_, err := rand.Read(randomBytes)
	if err != nil {
		fmt.Println("Error generating random bytes:", err)
		return nil
	}
	return randomBytes
}
