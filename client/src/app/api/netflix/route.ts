import { NextResponse } from "next/server";
import mysql from "mysql2/promise";

//* DB connection setup
const db = mysql.createPool({
  host: process.env.DATABASE_HOST,
  user: process.env.DATABASE_USER,
  password: process.env.DATABASE_PASSWORD,
  database: process.env.DATABASE_SCHEMA,
});

export async function GET(request: Request) {
  try {
    // Parse the search parameter from the request URL
    const { searchParams } = new URL(request.url);
    const search = searchParams.get("search") || "";

    //? Prepare the SQL query to fetch data from the database
    const query = `
      SELECT 
        s.show_id,
        s.title,
        s.type,
        s.release_year,
        s.rating,
        s.duration,
        s.date_added,
        s.description,
        GROUP_CONCAT(DISTINCT d.name) as directors,
        GROUP_CONCAT(DISTINCT c.name) as countries,
        GROUP_CONCAT(DISTINCT cat.name) as categories
      FROM titles s
      LEFT JOIN titles_directors sd ON s.show_id = sd.show_id
      LEFT JOIN directors d ON sd.director_id = d.id
      LEFT JOIN titles_countries sc ON s.show_id = sc.show_id
      LEFT JOIN countries c ON sc.country_id = c.id
      LEFT JOIN titles_categories scat ON s.show_id = scat.show_id
      LEFT JOIN categories cat ON scat.category_id = cat.id
      ${search ? "WHERE s.title LIKE ?" : ""}
      GROUP BY s.show_id
      ORDER BY s.title ASC
    `;

    // Execute the query with the search parameter if provided
    // Using parameterized queries to prevent SQL injection
    const params = search ? [`%${search}%`] : [];
    const [rows] = await db.execute(query, params);

    // Transform the results to match our interface
    const transformedRows = (rows as any[]).map((row) => ({
      ...row,
      directors: row.directors ? row.directors.split(",") : [],
      countries: row.countries ? row.countries.split(",") : [],
      categories: row.categories ? row.categories.split(",") : [],
    }));

    return NextResponse.json({ data: transformedRows });
  } catch (error) {
    console.error("Database error:", error);
    return NextResponse.json({ error: "Error fetching data" }, { status: 500 });
  }
}
