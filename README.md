Here is the `README.md` content based on the project files provided.

---

# SQLly

**SQLly** is a lightweight, file-based Relational Database Management System (RDBMS) running on **.NET 9.0**. It provides a custom SQL query engine with support for basic CRUD operations, joins, and schema management, accessible via a web-based console or a server-side REPL.

## Features

* **Custom Storage Engine:** Persists data to binary files (`.db`) with support for Primary Keys and Unique constraints.
* **SQL Support:** Handles `CREATE`, `DROP`, `INSERT`, `SELECT` (including `JOIN`), `UPDATE`, and `DELETE` commands.
* **Web Interface:** Includes a built-in web console for executing queries and a "Table View" to inspect raw data grids.
* **Dual Interaction:** Interact via the browser-based UI or the terminal-based REPL.
* **Theme Support:** Light and Dark mode toggle.

## Requirements

Before running this project, ensure you have the following installed:

* **[.NET 9.0 SDK](https://dotnet.microsoft.com/download/dotnet/9.0)**: The project targets the latest .NET 9 framework.
* **IDE:** Visual Studio 2022 (latest preview/update supporting .NET 9) or Visual Studio Code.

## Setup and Installation

1. **Clone the repository:**
```bash
git clone <repository-url>
cd bethteshs/sqlly/SQLly-b135c2bad768d6c7fb5add7ef94b0fc9e987302c

```


2. **Navigate to the project directory:**
```bash
cd RDBMS

```


3. **Restore dependencies:**
```bash
dotnet restore

```



## How to Run

### Option 1: Using the Command Line (CLI)

1. From the `RDBMS` directory, run the application:
```bash
dotnet run

```


2. The application will start the web server. By default, it listens on:
* HTTP: `http://localhost:5220`
* HTTPS: `https://localhost:7254`


3. Open your web browser and navigate to `http://localhost:5220` to access the SQLly Web Console.

### Option 2: Using Visual Studio

1. Open the solution file `RDBMS.sln` in Visual Studio.
2. Ensure `RDBMS` is set as the startup project.
3. Press **F5** or click the **Run** button (configured for the "http" or "https" profile).

## Usage

### Web Console

Once the application is running, the browser interface allows you to:

* **Create Databases:** Use the sidebar to create distinct database files.
* **Execute SQL:** Type commands into the console (e.g., `SELECT * FROM users`).
* **View Data:** Switch to the "Table View" tab to see a rendered HTML table of your data.

### Server-Side REPL

When the application is running, you can also interact directly with the "default" database via the terminal window where `dotnet run` is executing:

```text
SQLly REPL READY
SQLly> INSERT INTO users VALUES (1, "Admin", 99)

```

## Project Structure

* **`RDBMS/Engine/`**: Contains the core database logic (`Database.cs`, `StorageEngine.cs`, `TableSchema.cs`).
* **`RDBMS/wwwroot/`**: Contains the frontend static files (`index.html`, `style.css`, `script.js`).
* **`RDBMS/Program.cs`**: The entry point that configures the ASP.NET Core web host and API endpoints.

## Credits & Acknowledgments

* **Framework:** Built using [Microsoft .NET 9.0](https://dotnet.microsoft.com/) and ASP.NET Core.
* **Icons:** The application uses custom logo assets located in `wwwroot/images/`.
* **Fonts:** The UI utilizes the "Nunito Sans" font family.
