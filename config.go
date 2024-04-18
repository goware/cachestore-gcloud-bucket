package cachestoregcloudbucket

type Config struct {
	Enabled bool   `toml:"enabled"`
	Bucket  string `toml:"bucket"`
}
