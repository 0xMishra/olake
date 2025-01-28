package logger

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"

	"github.com/datazip-inc/olake/logger/console"
	"github.com/datazip-inc/olake/types"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	"gopkg.in/natefinch/lumberjack.v2"
)

var logger zerolog.Logger

func InitializeLogger(logFilePath string) zerolog.Logger {
	// Configure lumberjack for log rotation
	rotatingFile := &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    10,
		MaxBackups: 5,
		MaxAge:     30,
		Compress:   true,
	}
	// Create a multiwriter to log both console and file
	multiwriter := zerolog.MultiLevelWriter(os.Stdout, rotatingFile)

	logger := zerolog.New(multiwriter).With().Timestamp().Logger()

	return logger
}

func Init() {
	logger = InitializeLogger(fmt.Sprintf("%s/logs/olake.log", viper.GetString("configFolder")))
}

// Info writes record into os.stdout with log level INFO
func Info(v ...interface{}) {
	logger.Info().Msgf("%v", v...)
}

// Info writes record into os.stdout with log level INFO
func Infof(format string, v ...interface{}) {
	logger.Info().Msgf(format, v...)
}

// Debug writes record into os.stdout with log level DEBUG
func Debug(v ...interface{}) {
	logger.Debug().Msgf("%v", v...)
}

// Debugf writes record into os.stdout with log level DEBUG
func Debugf(format string, v ...interface{}) {
	logger.Debug().Msgf(format, v...)
}

// Error writes record into os.stdout with log level ERROR
func Error(v ...interface{}) {
	logger.Error().Msgf("%v", v...)
}

// Fatal writes record into os.stdout with log level ERROR and exits
func Fatal(v ...interface{}) {
	logger.Fatal().Msgf("%v", v...)
	os.Exit(1)
}

// Fatal writes record into os.stdout with log level ERROR
func Fatalf(format string, v ...interface{}) {
	logger.Fatal().Msgf(format, v...)
	os.Exit(1)
}

// Error writes record into os.stdout with log level ERROR
func Errorf(format string, v ...interface{}) {
	logger.Error().Msgf(format, v...)
}

// Warn writes record into os.stdout with log level WARN
func Warn(v ...interface{}) {
	logger.Warn().Msgf("%v", v...)
}

// Warn writes record into os.stdout with log level WARN
func Warnf(format string, v ...interface{}) {
	logger.Warn().Msgf(format, v...)
}

func LogSpec(spec map[string]interface{}) {
	message := types.Message{}
	message.Spec = spec
	message.Type = types.SpecMessage

	Info("logging spec")
	err := console.Print(console.INFO, message)
	if err != nil {
		Fatalf("failed to encode spec %v: %s", spec, err)
	}
}

func LogCatalog(streams []*types.Stream) {
	message := types.Message{}
	message.Type = types.CatalogMessage
	message.Catalog = types.GetWrappedCatalog(streams)
	Info("logging catalog")
	err := console.Print(console.INFO, message)
	if err != nil {
		Fatalf("failed to encode catalog %v: %s", streams, err)
	}

	// write catalog to the specified file
	if configFolder := viper.GetString("configFolder"); configFolder != "" {
		err = logFile(message.Catalog, configFolder, "catalog", ".json")
		if err != nil {
			Fatalf("failed to create catalog file: %v", err)
		}
	}
}
func LogConnectionStatus(err error) {
	message := types.Message{}
	message.Type = types.ConnectionStatusMessage
	message.ConnectionStatus = &types.StatusRow{}
	if err != nil {
		message.ConnectionStatus.Message = err.Error()
		message.ConnectionStatus.Status = types.ConnectionFailed
	} else {
		message.ConnectionStatus.Status = types.ConnectionSucceed
	}

	err = console.Print(console.INFO, message)
	if err != nil {
		Fatalf("failed to encode connection status: %s", err)
	}
}

func LogResponse(response *http.Response) {
	respDump, err := httputil.DumpResponse(response, true)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(respDump))
}

func LogRequest(req *http.Request) {
	requestDump, err := httputil.DumpRequest(req, true)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(requestDump))
}

func LogState(state *types.State) {
	state.Lock()
	defer state.Unlock()

	message := types.Message{}
	message.Type = types.StateMessage
	message.State = state

	err := console.Print(console.INFO, message)
	if err != nil {
		Fatalf("failed to encode connection status: %s", err)
	}
	if configFolder := viper.GetString("configFolder"); configFolder != "" {
		err = logFile(state, configFolder, "state", ".json")
		if err != nil {
			Fatalf("failed to create state file: %v", err)
		}
	}
}

// CreateFile creates a new file or overwrites an existing one with the specified filename, path, extension,
func logFile(content any, filePath string, fileName, fileExtension string) error {
	// Construct the full file path
	contentBytes, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("failed to marshal content: %v", err)
	}

	fullPath := filepath.Join(filePath, fileName+fileExtension)

	// Create or truncate the file
	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create or open file: %v", err)
	}
	defer file.Close()

	// Write data to the file
	_, err = file.Write(contentBytes)
	if err != nil {
		return fmt.Errorf("failed to write data to file: %v", err)
	}

	return nil
}
