# wm-request-queue


## Installing go with home brew and setting up the environment (Mac)

Update & Install Go
```
brew update && brew install golang
```

Setup workspace
```
mkdir -p $HOME/go/{bin,src,pkg}
```

Setup Environment

Add following lines to `~/.bash_profile`
```
export GOPATH=$HOME/go                                                           
export GOROOT="$(brew --prefix golang)/libexec"                                  
export GOBIN=$GOPATH/bin                                                         
export PATH=$PATH:$GOPATH/bin                                                    
export PATH=$PATH:$GOROOT/bin
```

## Development
- Clone the repository
- Run `make install`
- Run `make run`
