# wm-message-queue

## Install
```console
go get gitlab.uncharted.software/WM/wm-message-queue
```

If there are problems fetch the `dque` module, make sure that your environment includes the following:

```shell
export GOPRIVATE=gitlab.uncharted.software
```

Your `.gitconfig` may also need the following entry to get your `go get` command to work with SSH:

```shell
[url "ssh://git@gitlab.uncharted.software/"]
	insteadOf = https://gitlab.uncharted.software/
```

## Development
- Clone the repository
- Run `make install`
- Run `make build`

