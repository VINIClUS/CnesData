package main

import (
	"context"
	"fmt"
	"os"

	_ "github.com/nakagami/firebirdsql"

	"github.com/cnesdata/dumpagent/internal/platform"
)

var Version = "dev"

func main() {
	os.Exit(dispatch(os.Args[1:]))
}

func dispatch(args []string) int {
	if len(args) == 0 {
		return detectAutoMode()
	}
	cmd := args[0]
	rest := args[1:]
	switch cmd {
	case "run":
		return cmdRun(rest)
	case "service":
		return cmdService(rest)
	case "install":
		return cmdInstall(rest)
	case "uninstall":
		return cmdUninstall()
	case "version", "--version", "-v":
		return runVersion()
	case "help", "--help", "-h":
		printHelp()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		printHelp()
		return 2
	}
}

func cmdRun(args []string) int {
	verbose := hasFlag(args, "-v", "--verbose")
	ctx, cancel := platform.NotifyShutdown(context.Background())
	defer cancel()
	return runForeground(ctx, verbose)
}

func hasFlag(args []string, flags ...string) bool {
	for _, a := range args {
		for _, f := range flags {
			if a == f {
				return true
			}
		}
	}
	return false
}

func printHelp() {
	fmt.Println(`dumpagent — CnesData edge agent

Usage:
  dumpagent <command> [flags]

Commands:
  run          Executar em foreground (dev/debug)
  service      (interno) Chamado pelo SCM do Windows
  install      Registrar como Windows Service
  uninstall    Remover do Windows Service
  version      Imprimir versão
  help         Mostrar esta ajuda

Flags comuns:
  -v, --verbose    Logging DEBUG`)
}
