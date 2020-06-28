use std::collections::HashMap;
use std::env;
// use std::fs::File;
// use std::io::Read;
use std::path::PathBuf;

fn main() {
    println!("Let's wasmercise!");

    // Vocabulary:
    // assert_exists(source)
    // assert_value(var)is(val)
    // read(source)to(var)
    // write(val)to(dest)
    // exit
    // panic(val)
    //
    // source := file:foo or env:foo
    // dest := file:foo or stm:stdout or stm:stderr
    // var := var:foo
    // val := text:foo or var:foo

    let args: Vec<String> = env::args().collect();
    let mut test_context = TestContext::new(Environment::real());

    for arg in args {
        test_context.process_command_text(arg);
    }

    // // open a path using the hostpath volume
    // let path = Path::new("/mnt/storage/bacon_ipsum.txt");
    // let display = path.display();

    // let mut file = match File::open(&path) {
    //     Err(why) => panic!("couldn't open {}: {}", display,
    //                                                why),
    //     Ok(file) => file,
    // };

    // let mut contents = String::new();
    // file.read_to_string(&mut contents).expect(format!("could not read {}", display).as_str());
    // println!("{}", contents);

    println!("That's enough wasmercising for now; see you next test!");
}

struct Environment {
    pub get_env_var: fn(name: String) -> Result<String, std::env::VarError>,
    pub file_exists: fn(path: &PathBuf) -> bool,
    pub file_content: fn(path: &PathBuf) -> std::io::Result<String>,
}

impl Environment {
    fn real() -> Self {
        Self {
            get_env_var: |name| std::env::var(name),
            file_exists: |path| path.exists(),
            file_content: |path| std::fs::read_to_string(path),
        }
    }
}

struct TestContext {
    variables: HashMap<String, String>,
    environment: Environment,
}

impl TestContext {
    fn new(environment: Environment) -> Self {
        TestContext {
            variables: Default::default(),
            environment,
        }
    }

    fn process_command_text(&mut self, command_text: String) {
        match Command::parse(command_text.clone()) {
            Ok(command) => self.process_command(command),
            Err(e) => panic!("Error parsing command '{}': {}", command_text, e),
        }
    }

    fn process_command(&mut self, command: Command) {
        match command {
            Command::AssertExists(source) => self.assert_exists(source),
            Command::Read(source, destination) => self.read(source, destination),
        }
    }

    fn assert_exists(&mut self, source: DataSource) {
        match source {
            DataSource::File(path) => self.assert_file_exists(PathBuf::from(path)),
            DataSource::Env(name) => self.assert_env_var_exists(name),
        }
    }

    fn read(&mut self, source: DataSource, destination: Variable) {
        let content = match source {
            DataSource::File(path) => self.file_content(PathBuf::from(path)),
            DataSource::Env(name) => self.env_var_value(name),
        };
        let Variable(dest_name) = destination;
        self.variables.insert(dest_name, content);
    }

    fn assert_file_exists(&self, path: PathBuf) {
        if !(self.environment.file_exists)(&path) {
            panic!(
                "File {} was expected to exist but did not",
                path.to_string_lossy()
            );
        }
    }

    fn assert_env_var_exists(&self, name: String) {
        (self.environment.get_env_var)(name).unwrap();
    }

    fn file_content(&self, path: PathBuf) -> String {
        (self.environment.file_content)(&path).unwrap()
    }

    fn env_var_value(&self, name: String) -> String {
        (self.environment.get_env_var)(name).unwrap()
    }
}

#[derive(Debug, PartialEq)]
enum Command {
    AssertExists(DataSource),
    Read(DataSource, Variable),
}

impl Command {
    fn parse(text: String) -> anyhow::Result<Self> {
        let tokens = CommandToken::parse(text)?;
        match &tokens[0] {
            CommandToken::Bracketed(t) => {
                Err(anyhow::anyhow!("don't put commands in brackets: {}", t))
            }
            CommandToken::Plain(t) => match &t[..] {
                "assert_exists" => Self::parse_assert_exists(&tokens),
                "read" => Self::parse_read(&tokens),
                _ => Err(anyhow::anyhow!("unrecognised command: {}", t)),
            },
        }
    }

    fn parse_assert_exists(tokens: &[CommandToken]) -> anyhow::Result<Self> {
        match &tokens[..] {
            [_, CommandToken::Bracketed(source)] => {
                Ok(Self::AssertExists(DataSource::parse(source.to_string())?))
            }
            _ => Err(anyhow::anyhow!("unexpected assert_exists command syntax")),
        }
    }

    fn parse_read(tokens: &[CommandToken]) -> anyhow::Result<Self> {
        match &tokens[..] {
            // TODO: enforce that the separator is 'to'
            [_, CommandToken::Bracketed(source), CommandToken::Plain(_sep), CommandToken::Bracketed(destination)] => {
                Ok(Self::Read(
                    DataSource::parse(source.to_string())?,
                    Variable::parse(destination.to_string())?,
                ))
            }
            _ => Err(anyhow::anyhow!("unexpected read command syntax")),
        }
    }
}

#[derive(Debug, PartialEq)]
enum DataSource {
    File(String),
    Env(String),
}

#[derive(Debug, PartialEq)]
struct Variable(String);

impl DataSource {
    fn parse(text: String) -> anyhow::Result<Self> {
        let bits: Vec<&str> = text.split(':').collect();
        match bits[..] {
            ["file", f] => Ok(DataSource::File(f.to_string())),
            ["env", e] => Ok(DataSource::Env(e.to_string())),
            _ => Err(anyhow::anyhow!("invalid data source")),
        }
    }
}

impl Variable {
    fn parse(text: String) -> anyhow::Result<Self> {
        let bits: Vec<&str> = text.split(':').collect();
        match bits[..] {
            ["var", v] => Ok(Variable(v.to_string())),
            _ => Err(anyhow::anyhow!("invalid variable reference")),
        }
    }
}

#[derive(Debug, PartialEq)]
enum CommandToken {
    Plain(String),
    Bracketed(String),
}

impl CommandToken {
    fn parse(text: String) -> anyhow::Result<Vec<Self>> {
        if text.starts_with('(') {
            match text.find(')') {
                None => Err(anyhow::anyhow!("unmatched opening parenthesis: {}", text)),
                Some(close_index) => {
                    let bracketed_text = &text[1..close_index];
                    let rest = &text[close_index + 1..];
                    let mut parsed_rest = if !rest.is_empty() {
                        Self::parse(rest.to_string())?
                    } else {
                        Vec::new()
                    };
                    parsed_rest.insert(0, Self::Bracketed(bracketed_text.to_string()));
                    Ok(parsed_rest)
                }
            }
        } else {
            match text.find('(') {
                None => Ok(vec![Self::Plain(text)]),
                Some(open_index) => {
                    let plain_text = &text[0..open_index];
                    let rest = &text[open_index..];
                    let mut parsed_rest = Self::parse(rest.to_string())?;
                    parsed_rest.insert(0, Self::Plain(plain_text.to_string()));
                    Ok(parsed_rest)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // We want to pass literal strings so having something that accepts &str
    // instead of String reduces clutter
    fn parse_command(text: &str) -> anyhow::Result<Command> {
        Command::parse(text.to_owned())
    }
    fn parse_tokens(text: &str) -> anyhow::Result<Vec<CommandToken>> {
        CommandToken::parse(text.to_owned())
    }

    fn fake_env() -> Environment {
        Environment {
            get_env_var: |name| {
                if name == "test1" {
                    Ok("one".to_owned())
                } else {
                    Err(std::env::VarError::NotPresent)
                }
            },
            file_exists: |path| path.to_string_lossy() == "/fizz/buzz.txt",
            file_content: |path| {
                if path.to_string_lossy() == "/fizz/buzz.txt" {
                    Ok("fizzbuzz!".to_owned())
                } else {
                    Err(std::io::Error::from(std::io::ErrorKind::NotFound))
                }
            },
        }
    }

    #[test]
    fn tokenise_one_plain() {
        let tokens = parse_tokens("fie").expect("Unexpected parsing error");
        assert_eq!(1, tokens.len());
        assert_eq!(CommandToken::Plain("fie".to_owned()), tokens[0]);
    }

    #[test]
    fn tokenise_one_bracketed() {
        let tokens = parse_tokens("(fie)").expect("Unexpected parsing error");
        assert_eq!(1, tokens.len());
        assert_eq!(CommandToken::Bracketed("fie".to_owned()), tokens[0]);
    }

    #[test]
    fn tokenise_two() {
        let tokens = parse_tokens("assert_exists(file:foo)").expect("Unexpected parsing error");
        assert_eq!(2, tokens.len());
        assert_eq!(CommandToken::Plain("assert_exists".to_owned()), tokens[0]);
        assert_eq!(CommandToken::Bracketed("file:foo".to_owned()), tokens[1]);
    }

    #[test]
    fn tokenise_three() {
        let tokens = parse_tokens("foo(bar)quux").expect("Unexpected parsing error");
        assert_eq!(3, tokens.len());
        assert_eq!(CommandToken::Plain("foo".to_owned()), tokens[0]);
        assert_eq!(CommandToken::Bracketed("bar".to_owned()), tokens[1]);
        assert_eq!(CommandToken::Plain("quux".to_owned()), tokens[2]);
    }

    #[test]
    fn tokenise_four() {
        let tokens = parse_tokens("read(file:foo)to(var:ftext)").expect("Unexpected parsing error");
        assert_eq!(4, tokens.len());
        assert_eq!(CommandToken::Plain("read".to_owned()), tokens[0]);
        assert_eq!(CommandToken::Bracketed("file:foo".to_owned()), tokens[1]);
        assert_eq!(CommandToken::Plain("to".to_owned()), tokens[2]);
        assert_eq!(CommandToken::Bracketed("var:ftext".to_owned()), tokens[3]);
    }

    #[test]
    fn parse_single_assert() {
        let command = parse_command("assert_exists(file:foo)").expect("Unexpected parsing error");
        match command {
            Command::AssertExists(DataSource::File(f)) => {
                assert_eq!(f, "foo", "Expected file 'foo' but got {}", f)
            }
            _ => assert!(false, "Expected AssertExists but got {:?}", command),
        }
    }

    #[test]
    fn parse_single_read() {
        let command =
            parse_command("read(file:foo)to(var:ftext)").expect("Unexpected parsing error");
        match command {
            Command::Read(DataSource::File(f), Variable(v)) => {
                assert_eq!(f, "foo", "Expected source file 'foo' but got {}", f);
                assert_eq!(v, "ftext", "Expected dest var 'ftext' but got {}", v);
            }
            _ => assert!(false, "Expected Read but got {:?}", command),
        }
    }

    #[test]
    fn process_assert_file_exists_ok_when_exists() {
        let mut context = TestContext::new(fake_env());
        context.process_command_text("assert_exists(file:/fizz/buzz.txt)".to_owned());
    }

    #[test]
    #[should_panic]
    fn process_assert_file_exists_panics_when_doesnt_exist() {
        let mut context = TestContext::new(fake_env());
        context.process_command_text("assert_exists(file:/nope/nope/nope)".to_owned());
    }

    #[test]
    fn process_assert_env_var_exists_ok_when_exists() {
        let mut context = TestContext::new(fake_env());
        context.process_command_text("assert_exists(env:test1)".to_owned());
    }

    #[test]
    #[should_panic]
    fn process_assert_env_var_exists_panics_when_doesnt_exist() {
        let mut context = TestContext::new(fake_env());
        context.process_command_text("assert_exists(env:nope)".to_owned());
    }

    #[test]
    fn process_read_file_updates_when_exists() {
        let mut context = TestContext::new(fake_env());
        context.process_command_text("read(file:/fizz/buzz.txt)to(var:ftest)".to_owned());
        assert_eq!(context.variables.get("ftest").unwrap(), "fizzbuzz!");
    }

    #[test]
    fn process_read_env_var_updates_when_exists() {
        let mut context = TestContext::new(fake_env());
        context.process_command_text("read(env:test1)to(var:etest)".to_owned());
        assert_eq!(context.variables.get("etest").unwrap(), "one");
    }
}
