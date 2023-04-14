package supporting

import (
	"fmt"
	"github.com/urfave/cli"
)

func AdaptError(err error, exitCode int) *cli.ExitError {
	if err == nil {
		return nil
	}
	if e, ok := err.(*cli.ExitError); ok {
		return e
	}
	return cli.NewExitError(err.Error(), exitCode)
}

func AdaptErrorWithMessage(err error, msg string, exitCode int) *cli.ExitError {
	if err == nil {
		return nil
	}
	if e, ok := err.(*cli.ExitError); ok {
		return e
	}
	return cli.NewExitError(fmt.Sprintf("%s => err: %s", msg, err.Error()), exitCode)
}
