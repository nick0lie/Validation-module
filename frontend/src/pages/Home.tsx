import { useState } from "react";
import ConnectionForm from "../components/ConnectionForm";
import ConnectionList from "../components/ConnectionList";

function Home() {
  const [editingConn, setEditingConn] = useState(null);
  const [showForm, setShowForm] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);

  const handleEdit = (conn: any) => {
    setEditingConn(conn);
    setShowForm(true);
  };

  const handleAdd = () => {
    setEditingConn(null);
    setShowForm(true);
  };

  const handleSave = () => {
    setShowForm(false);
    setRefreshKey(prev => prev + 1);
  };

  return (
    <div
      style={{
        backgroundColor: "#dae6f2",
        minHeight: "100vh",
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
        padding: "2rem",
        boxSizing: "border-box"
      }}
    >

      <div
        style={{
          fontFamily: "sans-serif",
          maxWidth: 800,
          margin: "2rem auto",
          padding: "2rem",
          backgroundColor: "white",
          borderRadius: "8px",
          boxShadow: "0 0 10px rgba(190, 237, 251, 0.1)",
        }}
      >
        <h2 style={{ fontSize: "1.75rem", marginBottom: "1rem" }}>
          Выберите подключение или создайте новое
        </h2>

        {!showForm && (
          <>
            <ConnectionList key={refreshKey} onEdit={handleEdit} />
            <div style={{ display: "flex", gap: "1rem", marginTop: "1.5rem" }}>
              <button
                onClick={handleAdd}
                style={{
                  padding: "0.5rem 1rem",
                  fontSize: "1rem",
                  backgroundColor: "#4CAF50",
                  color: "white",
                  border: "none",
                  borderRadius: "4px",
                  cursor: "pointer",
                }}
              >
                + Добавить подключение
              </button>
              <a
                href="/reports"
                style={{
                  display: "inline-block",
                  padding: "0.5rem 1rem",
                  backgroundColor: "#4CAF50",
                  color: "white",
                  borderRadius: "4px",
                  textDecoration: "none",
                  fontSize: "1rem",
                }}
              >
                Отчёты
              </a>
            </div>
          </>
        )}

        {showForm && (
          <ConnectionForm
            existing={editingConn}
            onSave={handleSave}
            onCancel={() => setShowForm(false)}
          />
        )}
      </div>
    </div>
  );
}

export default Home;
