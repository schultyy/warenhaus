# Warenhaus

To store your data

## Development

- Rust.1.66 or later
- AssemblyScript Compiler (`npm install -g asc`)

```
$ cargo build
```

Create all necessary directories in the application root directory:

```bash
$ mkdir -p {db,queries}
```

And then:

```
$ RUST_LOG=debug ASM_SCRIPT_COMPILER_PATH=$(which asc) cargo run -p warenhaus
```

We use [tracing](https://github.com/tokio-rs/tracing) under the hood and use `RUST_LOG=debug` to see all debug messages.

Once running, the application listens on [`http://localhost:3030`](http://localhost:3030).

Also, we rely on the AssemblyScript compiler to be present on the machine. We provide the path to the binary via the `ASM_SCRIPT_COMPILER_PATH` variable.

### Test storing a new record:

```bash
$ curl -v -XPOST localhost:3030/index -H "Content-Type: application/json" -d '{"fields": ["url", "imestamp"], "values": ["https://google.com", 5454353]}'
```

### Querying Data

Before we can query data, we need to create a query. Create a new `map.ts` file:

```typescript
export function run(timestamp: i32) : bool {
    return true;
}
```

Then, add the file to the available queries via:

```
$ curl -XPOST -F 'data=@query.ts' http://localhost:3030/add_map/query -v  
```

Once this finished successfully, you can query data:

```bash
$ curl -XGET localhost:3030/query/query
[
  [
    {
      "Int": 1677125260
    },
    {
      "String": "http://21-lessons.con"
    },
    {
      "String": "Personal Website"
    }
  ],
  [
    {
      "Int": 1677125260
    },
    {
      "String": "http://cisco.com"
    },
    {
      "String": "Work Website"
    }
  ]
]
```

Syntax: `localhost:3030/query/<name of wasm function>`

### Database Schema

warenhaus reads schema files from `schema.json` in the root directory. 

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

