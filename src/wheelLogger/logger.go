package wheelLogger

import (
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"time"
)

type OCRLogger struct {
	logrus.Logger
}

var Logger = logrus.New()

func init() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	f, _ := os.Create("time-wheel-" + time.Now().Format("2006-01-02") + ".log")
	Logger.SetOutput(io.MultiWriter(f, os.Stdout))
	Logger.SetLevel(logrus.InfoLevel)
	//Logger.AddHook(NewFieldsHook("web", true, true))
}
