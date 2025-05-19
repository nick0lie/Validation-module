import { useEffect, useState } from "react";

type Connection = {
  id: number;
  name: string;
  host: string;
  port: number;
  dbname: string;
  user: string;
};

function ConnectionList({
  onEdit,
}: {
  onEdit: (conn: Connection) => void;
}) {
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
      <h3>Сохранённые подключения</h3>
      {connections.length === 0 && <p>Нет подключений</p>}
      <ul>
        {connections.map((conn) => (
          <li key={conn.id}>
            <strong>{conn.name}</strong> ({conn.host}:{conn.port}/{conn.dbname}){" "}
            <button onClick={() => onEdit(conn)}>✏️</button>{" "}
            <button onClick={() => deleteConnection(conn.id)}>🗑</button>{" "}
            <button onClick={() => handleGoToChecks(conn.name)}>🔍 Проверки</button>
          </li>
        ))}
      </ul>
      <div style={{ color: "red" }}>{message}</div>
    </div>
  );
}

export default ConnectionList;
