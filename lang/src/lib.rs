use anyhow::Result;
use wasmtime::*;

fn execute_map(code: &str) -> Result<()> {
    let engine = Engine::default();
    // let module = Module
    let module = Module::new(&engine, code)?;
    let mut store = Store::new(&engine, ());

    let log_func = Func::wrap(&mut store, |_caller: Caller<'_, ()>| {
        println!("Logging");
    });

    let imports = [log_func.into()];
    let instance = Instance::new(&mut store, &module, &imports)?;

    let run = instance.get_typed_func::<(), ()>(&mut store, "run")?;

    run.call(&mut store, ())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let wat = r#"
            (module
                (func $log (import "" "log"))

                (func (export "run")
                    call $log)
            )
        "#;
        let result = execute_map(wat);
        assert!(result.is_ok());
    }
}
