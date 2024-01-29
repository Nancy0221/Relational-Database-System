package repl

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	uuid "github.com/google/uuid"
)

// REPL struct.
type REPL struct {
	commands map[string]func(string, *REPLConfig) error
	help     map[string]string
}

// REPL Config struct.
type REPLConfig struct {
	writer   io.Writer
	clientId uuid.UUID
}

// Get writer.
func (replConfig *REPLConfig) GetWriter() io.Writer {
	return replConfig.writer
}

// Get address.
func (replConfig *REPLConfig) GetAddr() uuid.UUID {
	return replConfig.clientId
}

// Construct an empty REPL.
func NewRepl() *REPL {
	return &REPL{commands: make(map[string]func(string, *REPLConfig) error), help: make(map[string]string)}
	// panic("function not yet implemented");
}

// Combine a slice of REPLs. If no REPLs are passed in,
// return a NewREPL(). If REPLs have overlapping triggers,
// return an error. Otherwise, return a REPL with the union
// of the triggers.
func CombineRepls(repls []*REPL) (*REPL, error) {
	// nothing in repls
	if len(repls) == 0 {
		return NewRepl(), nil
	}
	// handling with overlapping triggers
	// creating empty commands and helps to store the unrepeated value
	var commandsMap = make(map[string]func(string, *REPLConfig) error)
	var helpsMap = make(map[string]string)
	// note: each val is a repl, and it has commands and helps
	for _, repl := range repls {
		// iterate a map, str is key, f is value
		for str, f := range repl.GetCommands() {
			// check if current command exist in "commands"
			var _, check = commandsMap[str]
			if check {
				// having overlapping triggers
				return nil, errors.New("Having overlapping triggers: " + str)
			}
			commandsMap[str] = f
		}

		for keyStr, valStr := range repl.GetHelp() {
			// check if current command exist in "commands"
			var _, check = helpsMap[keyStr]
			if check {
				// having overlapping triggers
				return nil, errors.New("Having overlapping triggers: " + keyStr)
			}
			helpsMap[keyStr] = valStr
		}
	}
	return &REPL{commands: commandsMap, help: helpsMap}, nil
	// panic("function not yet implemented")
}

// Get commands.
func (r *REPL) GetCommands() map[string]func(string, *REPLConfig) error {
	return r.commands
}

// Get help.
func (r *REPL) GetHelp() map[string]string {
	return r.help
}

// Add a command, along with its help string, to the set of commands.
func (r *REPL) AddCommand(trigger string, action func(string, *REPLConfig) error, help string) {
	// commands is a map, map with trigger and action
	r.commands[trigger] = action
	// also update the help map
	r.help[trigger] = help
	// panic("function not yet implemented")
}

// Return all REPL usage information as a string.
func (r *REPL) HelpString() string {
	var sb strings.Builder
	for k, v := range r.help {
		sb.WriteString(fmt.Sprintf("%s: %s\n", k, v))
	}
	return sb.String()
}

// Run the REPL.
func (r *REPL) Run(c net.Conn, clientId uuid.UUID, prompt string) {
	// Get reader and writer; stdin and stdout if no conn.
	var reader io.Reader
	var writer io.Writer
	if c == nil {
		reader = os.Stdin
		writer = os.Stdout
	} else {
		reader = c
		writer = c
	}
	scanner := bufio.NewScanner((reader))
	replConfig := &REPLConfig{writer: writer, clientId: clientId}
	// add .help in repl
	var action = func(str string, replConfig *REPLConfig) error {
		// action logic: print the help infomation
		io.WriteString(writer, r.HelpString())
		return nil
	}
	r.AddCommand(".help", action, "get infomation about triggers. ")
	// print a prompt
	io.WriteString(writer, prompt)
	// Begin the repl loop!
	for scanner.Scan() {
		// end of the file || interrupt the program
		if scanner.Text() == "EOF" || scanner.Text() == "SIGINT" {
			break
		}
		// cleaning input, and use whitespaces to split
		var input = cleanInput(scanner.Text())
		// the first input string should be the trigger
		var split = strings.Fields(input)
		// user command
		var action, check = r.GetCommands()[split[0]]
		if !check {
			io.WriteString(writer, "Your trigger is invalid. Try again. \n")
			continue
		} else {
			// Retrieve the function associated with the command trigger "mycommand"
			commandFunc := action //myRepl.commands["mycommand"]
			// make corresponding action
			err := commandFunc(input, replConfig)
			if err != nil {
				// having error? print it
				io.WriteString(writer, fmt.Sprint(err))
			}
		}
		// print a prompt
		io.WriteString(writer, prompt)
	}
	// panic("function not yet implemented")
}

// Run the REPL.
func (r *REPL) RunChan(c chan string, clientId uuid.UUID, prompt string) {
	// Get reader and writer; stdin and stdout if no conn.
	writer := os.Stdout
	replConfig := &REPLConfig{writer: writer, clientId: clientId}
	// Begin the repl loop!
	io.WriteString(writer, prompt)
	for payload := range c {
		// Emit the payload for debugging purposes.
		io.WriteString(writer, payload+"\n")
		// Parse the payload.
		fields := strings.Fields(payload)
		if len(fields) == 0 {
			io.WriteString(writer, prompt)
			continue
		}
		trigger := cleanInput(fields[0])
		// Check for a meta-command.
		if trigger == ".help" {
			io.WriteString(writer, r.HelpString())
			io.WriteString(writer, prompt)
			continue
		}
		// Else, check user commands.
		if command, exists := r.commands[trigger]; exists {
			// Call a hardcoded function.
			err := command(payload, replConfig)
			if err != nil {
				io.WriteString(writer, fmt.Sprintf("%v\n", err))
			}
		} else {
			io.WriteString(writer, "command not found\n")
		}
		io.WriteString(writer, prompt)
	}
	// Print an additional line if we encountered an EOF character.
	io.WriteString(writer, "\n")
}

// cleanInput preprocesses input to the db repl.
func cleanInput(text string) string {
	output := strings.TrimSpace(text)
	output = strings.ToLower(output)
	return output
}
