package wheelLogger

import (
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"time"
	"strconv"
	"runtime"
	"strings"
)

type OCRLogger struct {
	logrus.Logger
}

var Logger = logrus.New()

type FieldsHook struct {
	SpanID    string `当前模块的spanid`
	WithTime  bool
	WithStack bool
}

func NewFieldsHook(spanID string, withTime bool, withStack bool) *FieldsHook {
	return &FieldsHook{
		SpanID:    spanID + strconv.Itoa(os.Getpid()),
		WithTime:  withTime,
		WithStack: withStack,
	}
}

func (hook *FieldsHook) Fire(entry *logrus.Entry) error {
	entry.Data["SpanID"] = hook.SpanID
	if hook.WithTime {
		entry.Data["log-time"] = time.Now().Format("2006-01-02 15:04:05.999")
	}
	if hook.WithStack {
		fileName, funcName, lineNo := getCaller()
		entry.Data["file"] = fileName
		entry.Data["function"] = funcName
		entry.Data["line"] = lineNo
	}
	return nil
}
func (hook *FieldsHook) Levels() []logrus.Level {
	return logrus.AllLevels
}
func formatFileName(name string) string {
	index := strings.LastIndex(name, "/")
	if index > 0 {
		return name[index+1:]
	}
	return name
}

var callLevel int

func getCaller() (file string, funcName string, line int) {
	found := false
	// 已知当前的库调用层次
	/*
		hooks.go github.com/sirupsen/logrus.LevelHooks.Fire 28 false 2
		entry.go github.com/sirupsen/logrus.(*Entry).fireHooks 223 false 3
		entry.go github.com/sirupsen/logrus.Entry.log 201 false 4
		entry.go github.com/sirupsen/logrus.(*Entry).Info 261 false 5
		views.go card/view.HandleRecognize 28 true 6
	*/
	if callLevel != 0 {
		if pc, fileName, lineNo, ok := runtime.Caller(callLevel); ok {
			file = formatFileName(fileName)
			funcName = runtime.FuncForPC(pc).Name()
			line = lineNo
		}
		return
	} else {
		for n := 5; n < 10 && !found; n++ {
			if pc, fileName, lineNo, ok := runtime.Caller(n); ok {
				if strings.Index(fileName, "logrus") < 0 && strings.Index(fileName, "logger/hook.go") < 0 {
					found = true

				}
				if n > 10 {
					// break
					found = true
				}
				callLevel = n
				file = formatFileName(fileName)
				funcName = runtime.FuncForPC(pc).Name()
				line = lineNo
			}
		}
	}
	return
}

func init() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	f, _ := os.Create("time-wheel-" + time.Now().Format("2006-01-02") + ".log")
	Logger.SetOutput(io.MultiWriter(f, os.Stdout))
	Logger.SetLevel(logrus.InfoLevel)
	Logger.AddHook(NewFieldsHook("timewheel", true, true))
}
