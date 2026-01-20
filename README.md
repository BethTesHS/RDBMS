# SQLly

**SQLly** is a lightweight, file-based Relational Database Management System (RDBMS) running on **Go (Golang)**. It provides a custom SQL query engine with support for basic CRUD operations, joins, and schema management, accessible via a web-based console or a server-side REPL.

## Features

* **Custom Storage Engine:** Persists data to binary files (`.db`) with support for Primary Keys and Unique constraints.
* **SQL Support:** Handles `CREATE`, `DROP`, `INSERT`, `SELECT` (including `JOIN`), `UPDATE`, and `DELETE` commands.
* **Web Interface:** Includes a built-in web console for executing queries and a "Table View" to inspect raw data grids.
* **Dual Interaction:** Interact via the browser-based UI or the terminal-based REPL.
* **Theme Support:** Light and Dark mode toggle.

## Requirements

Before running this project, ensure you have the following installed:

Go 1.23+: The project targets Go 1.23 or newer.

IDE: Visual Studio Code (recommended) or any Go-compatible editor.

* **[Go 1.23+](https://go.dev/dl/)**: The project targets Go 1.23 or newer.
* **IDE:** Visual Studio Code (recommended) or any Go-compatible editor.

## Setup and Installation

1. **Clone the repository and Navigate to the project directory**
```bash
cd sqlly-go

```
2. **Restore dependencies:**
```bash
go mod tidy

```

## How to Run

### Using the Command Line (CLI)

1. From the `sqlly-go` directory, run the application:
```bash
go run cmd/server/main.go

```


2. The application will start the web server. By default, it listens on:
* HTTP: `http://localhost:5220`


3. Open your web browser and navigate to `http://localhost:5220` to access the SQLly Web Console.

## Usage

### Web Console

Once the application is running, the browser interface allows you to:

* **Create Databases:** Use the sidebar to create distinct database files.
* **Execute SQL:** Type commands into the console (e.g., `SELECT * FROM users`).
* **View Data:** Switch to the "Table View" tab to see a rendered HTML table of your data.

## Credits & Acknowledgments

* **Framework:** Ported to Go (Golang)
* **Icons:** The application uses custom logo assets located in `wwwroot/images/`.
* **Fonts:** The UI utilizes the "Nunito Sans" font family.
