package models

type Configurations struct {
	Server   ServerConfigurations
	Database DatabaseConfigurations
	Logger   LoggerConfigurations
	Software SoftwareConfigurations
}

type ServerConfigurations struct {
	ADDRESS string
	PORT    int
}

type DatabaseConfigurations struct {
	DB_USER     string
	DB_PASSWORD string
}

type LoggerConfigurations struct {
	LOG_DIRECTORY string
	LOG_FILENAME  string
}

type SoftwareConfigurations struct {
	DESTINATION_PREFIX   string
	RESTORE_THREADS      int
	DUMP_FILE_DIRECTORYS string
}
