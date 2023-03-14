# 1. This tells docker to use the Rust official image
FROM rust:1.67

ENV RUST_LOG "info"

#this is a legacy environment variable
ENV ASM_SCRIPT_COMPILER_PATH "/dev/null"
ENV CONFIG_FILE_ROOT_PATH "/opt"

WORKDIR /usr/app
RUN USER=root cargo new --bin warenhaus
WORKDIR /usr/app/warenhaus

# 2. Copy the files in your machine to the Docker image
COPY ./Cargo.toml  ./Cargo.toml

RUN cargo build --release
RUN rm src/*.rs
COPY ./src ./src

# 5. Build for release.
RUN rm ./target/release/deps/warenhaus*
RUN cargo install --path .

CMD [ "warenhaus" ]

