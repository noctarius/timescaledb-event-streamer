package supporting

import "github.com/urfave/cli"

func AdaptError(err error, exitCode int) *cli.ExitError {
	if err == nil {
		return nil
	}
	if e, ok := err.(*cli.ExitError); ok {
		return e
	}
	return cli.NewExitError(err.Error(), exitCode)
}