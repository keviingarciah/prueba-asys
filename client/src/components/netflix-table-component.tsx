"use client";
import { useState, useEffect } from "react";
import { NetflixTitle } from "@/types/netflix.types";

export default function NetflixTable() {
  const [data, setData] = useState<NetflixTitle[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState("");

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        //* If searchTerm is empty, return early to avoid unnecessary API calls
        const response = await fetch(`/api/netflix?search=${searchTerm}`);
        const result = await response.json();
        setData(result.data || []);
      } catch (error) {
        console.error("Error:", error);
      }
      setLoading(false);
    };

    //* The debounce effect to limit API calls, especially on fast typing
    const debounceTimer = setTimeout(fetchData, 1000); // 1 second debounce
    return () => clearTimeout(debounceTimer);
  }, [searchTerm]);

  const columns: Array<{ key: keyof NetflixTitle; label: string }> = [
    { key: "show_id", label: "Show ID" },
    { key: "type", label: "Tipo" },
    { key: "title", label: "Título" },
    { key: "directors", label: "Director" },
    { key: "release_year", label: "Año de Lanzamiento" },
    { key: "rating", label: "Rating" },
    { key: "duration", label: "Duración" },
    { key: "date_added", label: "Año Añadido" },
    { key: "countries", label: "País" },
    { key: "categories", label: "Listado en" },
    { key: "description", label: "Descripción" },
  ];

  return (
    <div className="w-full bg-white rounded-lg shadow-xl border border-gray-200">
      <div className="p-6 border-b border-gray-200 bg-gray-50">
        <h2 className="text-2xl font-semibold text-gray-800 mb-4">
          Prueba ASYS
        </h2>

        {/* Input for searching titles */}
        <div className="relative">
          <input
            type="text"
            placeholder="Buscar por título..."
            className="w-full p-4 pl-12 border rounded-lg text-gray-700 bg-white shadow-sm 
                     focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent
                     transition-all duration-200"
            onChange={(e) => setSearchTerm(e.target.value)}
          />
          <svg
            className="absolute left-4 top-4 h-6 w-6 text-gray-400"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
            />
          </svg>
        </div>
      </div>

      <div className="overflow-x-auto">
        {loading ? (
          <div className="flex flex-col justify-center items-center h-64 bg-gray-50">
            <div className="animate-spin rounded-full h-16 w-16 border-4 border-blue-500 border-t-transparent" />
            <p className="mt-4 text-gray-600">Cargando resultados...</p>
          </div>
        ) : (
          <div className="min-w-full">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-900">
                <tr>
                  {columns.map(({ key, label }) => (
                    <th
                      key={key}
                      className="px-6 py-5 text-left text-xs font-semibold text-gray-100 uppercase 
                               tracking-wider whitespace-nowrap"
                    >
                      {label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {data.map((item: NetflixTitle) => (
                  <tr
                    key={item.show_id}
                    className="hover:bg-blue-50 transition-colors duration-200"
                  >
                    {columns.map(({ key }) => (
                      <td
                        key={`${item.show_id}-${key}`}
                        className="px-6 py-4 text-sm text-gray-700 whitespace-normal"
                      >
                        <div className="max-w-xs overflow-hidden">
                          {item[key]?.toString() || "-"}
                        </div>
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>

            {/* Show a message when no results are found */}
            {!loading && data.length === 0 && (
              <div className="flex flex-col items-center justify-center h-64 bg-gray-50">
                <svg
                  className="h-16 w-16 text-gray-400 mb-4"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M9.172 16.172a4 4 0 015.656 0M9 10h.01M15 10h.01M12 20a8 8 0 100-16 8 8 0 000 16z"
                  />
                </svg>
                <p className="text-gray-600 text-lg">
                  No se encontraron resultados
                </p>
                <p className="text-gray-400 text-sm mt-2">
                  Intenta con otros términos de búsqueda
                </p>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
