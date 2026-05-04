#### Starting OBS with MoQ enabled
`just run` in `./.reference/moq-obs/`

#### Starting the relay
`just relay` in `./.reference/moq/`

#### Starting the browser player
`just web serve http://localhost:4443/<root>` in `./.reference/moq/`
(might need to run `just install` beforehand!)

#### Starting the CLI monitoring subscriber
`cargo run -- https://localhost:4443/<root> <broadcast>` in `./standalone/`

#### Starting the reference CMAF passthrough publisher
```
just pub ffmpeg-cmaf "$(pwd)/dev/bbb.fmp4" | \
  cargo run --bin moq-cli -- --stats=60 publish --url http://localhost:4443/<root> --name <broadcast> fmp4
```
in `./.reference/moq`
