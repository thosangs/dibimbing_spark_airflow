DROP TABLE IF EXISTS retail;
CREATE TABLE IF NOT EXISTS retail (
    InvoiceNo TEXT,
    StockCode TEXT,
    Description TEXT,
    Quantity INTEGER,
    InvoiceDate DATE,
    UnitPrice FLOAT,
    CustomerID TEXT,
    Country TEXT
);
