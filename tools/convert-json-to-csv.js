import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DEFAULT_INPUT_DIR = path.join(__dirname, "..", "serg-JSON-CSV", "json");
const DEFAULT_OUTPUT_FILE = path.join(__dirname, "..", "data.csv");

const inputDir = process.argv[2]
  ? path.resolve(process.argv[2])
  : DEFAULT_INPUT_DIR;
const outputFile = process.argv[3]
  ? path.resolve(process.argv[3])
  : DEFAULT_OUTPUT_FILE;

async function collectJsonFiles(directory) {
  const entries = await fs.readdir(directory, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    const fullPath = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      const nestedFiles = await collectJsonFiles(fullPath);
      files.push(...nestedFiles);
    } else if (entry.isFile() && entry.name.toLowerCase().endsWith(".json")) {
      files.push(fullPath);
    }
  }

  return files.sort((a, b) => a.localeCompare(b));
}

function toStringValue(value) {
  if (value === undefined || value === null) {
    return "";
  }

  if (typeof value === "string") {
    return value;
  }

  return JSON.stringify(value);
}

function escapeCsv(value) {
  const stringValue = toStringValue(value);
  const needsQuoting = /[",\n]/.test(stringValue);
  const escaped = stringValue.replace(/"/g, '""');
  return needsQuoting ? `"${escaped}"` : escaped;
}

function buildRow(parsedJson) {
  const row = {
    id: toStringValue(parsedJson?.id),
    timestamp: toStringValue(parsedJson?.timestamp),
    url: toStringValue(parsedJson?.url),
  };

  if (!Array.isArray(parsedJson?.data)) {
    return row;
  }

  parsedJson.data
    .filter((item) => item && typeof item === "object" && !Array.isArray(item))
    .forEach((item, index) => {
      const keys = Object.keys(item);
      const urlKey = keys.find((key) => key.toLowerCase() === "url");
      const urlValue = urlKey ? toStringValue(item[urlKey]) : "";
      const dataKeys = keys.filter((key) => key !== urlKey);

      if (dataKeys.length === 0 && urlValue) {
        const fallbackUrlKey = `URL_${index + 1}`;
        if (!row[fallbackUrlKey]) {
          row[fallbackUrlKey] = urlValue;
        }
        return;
      }

      dataKeys.forEach((key) => {
        const columnKey = key.trim();
        const columnValue = toStringValue(item[key]);
        if (columnKey && !row[columnKey]) {
          row[columnKey] = columnValue;
        }

        if (urlValue) {
          const pairedUrlKey = `${columnKey}_URL`;
          if (!row[pairedUrlKey]) {
            row[pairedUrlKey] = urlValue;
          }
        }
      });
    });

  return row;
}

function buildColumns(rows) {
  const baseColumns = ["id", "timestamp", "url"];
  const extraColumns = new Set();

  rows.forEach((row) => {
    Object.keys(row).forEach((key) => {
      if (!baseColumns.includes(key)) {
        extraColumns.add(key);
      }
    });
  });

  return [...baseColumns, ...Array.from(extraColumns).sort((a, b) => a.localeCompare(b))];
}

async function writeCsv(rows, columns, destination) {
  const lines = [columns.join(",")];
  rows.forEach((row) => {
    const line = columns.map((column) => escapeCsv(row[column] ?? ""));
    lines.push(line.join(","));
  });

  await fs.writeFile(destination, `${lines.join("\n")}\n`, "utf-8");
}

async function main() {
  try {
    await fs.access(inputDir);
  } catch (error) {
    console.error(`Input directory not found: ${inputDir}`);
    process.exit(1);
  }

  const jsonFiles = await collectJsonFiles(inputDir);
  if (!jsonFiles.length) {
    console.error(`No JSON files found under ${inputDir}`);
    process.exit(1);
  }

  const rows = [];
  for (const filePath of jsonFiles) {
    try {
      const raw = await fs.readFile(filePath, "utf-8");
      const parsed = JSON.parse(raw);
      rows.push(buildRow(parsed));
    } catch (error) {
      console.error(`Failed to process ${filePath}: ${error.message}`);
    }
  }

  if (!rows.length) {
    console.error("No data rows were produced from the JSON files.");
    process.exit(1);
  }

  const columns = buildColumns(rows);
  await writeCsv(rows, columns, outputFile);
  console.log(`Saved ${rows.length} rows to ${outputFile}`);
}

main();
