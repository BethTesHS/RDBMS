using RDBMS.Engine;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

// 1. Register Database Manager (holds all databases)
var dbManager = new DatabaseManager();
builder.Services.AddSingleton(dbManager);

var app = builder.Build();

// 2. Start REPL for the "default" database (Server-side interactive mode)
new Repl(dbManager.GetDatabase("default")).Start();

// 3. Enable Static Files for the UI
app.UseDefaultFiles();
app.UseStaticFiles();

// 4. API Endpoints

// List all databases
app.MapGet("/api/dbs", (DatabaseManager mgr) => mgr.ListDatabases());

// Create a new database
app.MapPost("/api/dbs/{name}", (string name, DatabaseManager mgr) => 
{
    if (mgr.GetDatabase(name) != null) return Results.Conflict("Database exists");
    mgr.CreateDatabase(name);
    return Results.Ok($"Database {name} created.");
});

// Run SQL Query against a specific DB
app.MapPost("/api/query", ([FromQuery] string db, [FromBody] string sql, DatabaseManager mgr) => 
{
    var database = mgr.GetDatabase(db);
    if (database == null) return Results.NotFound("Database not found.");
    return Results.Ok(database.ExecuteSql(sql));
});

// NEW ENDPOINT: List Tables in a Database
app.MapGet("/api/dbs/{dbName}/tables", (string dbName, DatabaseManager mgr) => 
{
    var database = mgr.GetDatabase(dbName);
    if (database == null) return Results.NotFound("Database not found");
    return Results.Ok(database.Tables.Keys.ToList());
});

// DEBUG API: Get Table Data (raw JSON for the frontend grid)
app.MapGet("/api/dbs/{dbName}/tables/{tableName}", (string dbName, string tableName, DatabaseManager mgr) => 
{
    var database = mgr.GetDatabase(dbName);
    if (database == null) return Results.NotFound("Database not found");
    
    if (!database.Tables.ContainsKey(tableName)) return Results.NotFound("Table not found");

    // Extract data for JSON response
    var rows = database.Tables[tableName].SelectAll().Select(r => r.Data);
    return Results.Ok(rows);
});

// Fallback for SPA
app.MapFallbackToFile("index.html");

app.Run();