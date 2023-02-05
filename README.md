# Gringotts

To store your data

## Development

- Rust.1.66 or later


```
$ cargo build
```

And then:

```
$ RUST_LOG=Debug cargo run
```

We use [tracing](https://github.com/tokio-rs/tracing) under the hood and use `RUST_LOG=debug` to see all debug messages.

Once running, gringotts listens on [`http://localhost:3030`](http://localhost:3030).


Test storing a new record:

```bash
$ curl -v -XPOST localhost:3030/index -H "Content-Type: application/json" -d '{"fields": ["url", "imestamp"], "values": [{"String": "https://google.com"}, {"Int": 5454353}]}'
```

### Database Schema

gringotts reads schema files from `schema.json` in the root directory. 

Example:

```json
{
  "columns": [
    {
      "name": "Url",
      "data_type": "String"
    },
    {
      "name": "timestamp",
      "data_type": "Int"
    },
    {
      "name": "score",
      "data_type": "Float"
    }
  ]
}
```

Available Data Types:

| Type    | Corresponding Rust Type |
| ------- | ------------------------|
| Int     | `i64`                   |
| Float   | `f64`                   |
| String  | `std::String`           |
| Boolean | `bool`                  |

