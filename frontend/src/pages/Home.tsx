import { useState } from "react";
import ConnectionList from "../components/ConnectionList";
import ConnectionForm from "../components/ConnectionForm";
import CheckRunner from "../components/CheckRunner";

function Home() {
  const [editingConn, setEditingConn] = useState(null);
  const [showForm, setShowForm] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0); // триггер обновления

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
    setRefreshKey((prev) => prev + 1); // триггер обновления списка
  };

  return (
    <div style={{ padding: "2rem" }}>
      <h2>Выберите подключение или создайте новое</h2>

      {!showForm && (
        <>
          <ConnectionList key={refreshKey} onEdit={handleEdit} />
          <button onClick={handleAdd}>Добавить подключение</button>
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
  );
}

<CheckRunner />
export default Home;
