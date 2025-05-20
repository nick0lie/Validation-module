import { useEffect, useState } from "react";

type Connection = {
  id: number;
  name: string;
  host: string;
  port: number;
  dbname: string;
  user: string;
};

function ConnectionList({ onEdit }: { onEdit: (conn: Connection) => void }) {
  const [connections, setConnections] = useState<Connection[]>([]);
  const [message, setMessage] = useState("");

  const fetchConnections = async () => {
    try {
      const res = await fetch("http://localhost:8000/api/connections/");
      const data = await res.json();
      setConnections(data);
    } catch {
      setMessage("Ошибка загрузки подключений");
    }
  };

  useEffect(() => {
    fetchConnections();
  }, []);

  const deleteConnection = async (id: number) => {
    try {
      const res = await fetch(`http://localhost:8000/api/connections/${id}`, {
        method: "DELETE",
      });
      if (res.ok) {
        setConnections((prev) => prev.filter((c) => c.id !== id));
      } else {
        const data = await res.json();
        setMessage(data.detail || "Ошибка удаления");
      }
    } catch {
      setMessage("Ошибка удаления");
    }
  };

  const handleGoToChecks = (connName: string) => {
    localStorage.setItem("selectedConnection", connName);
    window.location.href = "/checks";
  };

  return (
    <div style={{ marginBottom: "20px" }}>
      {connections.length === 0 && <p>Нет подключений</p>}
      <ul style={{ listStyle: "none", padding: 0 }}>
        {connections.map((conn) => (
          <li
            key={conn.id}
            style={{
              marginBottom: "1rem",
              padding: "1rem",
              backgroundColor: "#f0f4f8",
              borderRadius: "6px",
              boxShadow: "0 0 4px rgba(0,0,0,0.1)",
            }}
          >
            <div style={{ marginBottom: "0.5rem" }}>
              <strong>{conn.name}</strong> ({conn.host}:{conn.port}/{conn.dbname})
            </div>
            <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap" }}>
              <button
                onClick={() => onEdit(conn)}
                style={{
                  padding: "0.4rem 0.8rem",
                  fontSize: "0.9rem",
                  backgroundColor: "#2196F3",
                  color: "white",
                  border: "none",
                  borderRadius: "4px",
                  cursor: "pointer",
                }}
              >
                ✏️ Редактировать
              </button>
              <button
                onClick={() => deleteConnection(conn.id)}
                style={{
                  padding: "0.4rem 0.8rem",
                  fontSize: "0.9rem",
                  backgroundColor: "#f44336",
                  color: "white",
                  border: "none",
                  borderRadius: "4px",
                  cursor: "pointer",
                }}
              >
                🗑 Удалить
              </button>
              <button
                onClick={() => handleGoToChecks(conn.name)}
                style={{
                  padding: "0.4rem 0.8rem",
                  fontSize: "0.9rem",
                  backgroundColor: "#4CAF50",
                  color: "white",
                  border: "none",
                  borderRadius: "4px",
                  cursor: "pointer",
                }}
              >
                🔍 Проверки
              </button>
            </div>
          </li>
        ))}
      </ul>
      {message && <div style={{ color: "red" }}>{message}</div>}
    </div>
  );
}

export default ConnectionList;
