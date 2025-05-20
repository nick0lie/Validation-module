import { useEffect, useState } from "react";

function ConnectionForm({
  existing,
  onSave,
  onCancel,
}: {
  existing?: any;
  onSave: () => void;
  onCancel: () => void;
}) {
  const [form, setForm] = useState({
    name: "",
    host: "",
    port: 5432,
    dbname: "",
    user: "",
    password: "",
  });

  const [message, setMessage] = useState("");

  useEffect(() => {
    if (existing) {
      setForm(existing);
    }
  }, [existing]);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setForm({ ...form, [e.target.name]: e.target.value });
  };

  const handleConnect = async () => {
    setMessage("");

    const { host, port, dbname, user, password } = form;
    const params = new URLSearchParams({
      host,
      port: String(port),
      dbname,
      user,
      password,
    }).toString();

    try {
      const testRes = await fetch(`http://localhost:8000/api/connections/test?${params}`);
      const testData = await testRes.json();

      if (!testRes.ok) {
        setMessage(testData.detail || "Ошибка при проверке");
        return;
      }

      const saveRes = await fetch("http://localhost:8000/api/connections/", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(form),
      });

      const saveData = await saveRes.json();

      if (!saveRes.ok) {
        setMessage(saveData.detail || "Ошибка сохранения");
      } else {
        setMessage("✅ Подключение успешно сохранено");
        onSave();
      }
    } catch {
      setMessage("Ошибка запроса");
    }
  };

  return (
    <div style={{ marginTop: "20px" }}>
      <h3 style={{ fontSize: "1.3rem", marginBottom: "1rem" }}>
        {existing ? "Редактировать подключение" : "Новое подключение"}
      </h3>
      <div style={{ display: "grid", gap: "12px", maxWidth: "400px" }}>
        {["name", "host", "port", "dbname", "user", "password"].map((field) => (
          <input
            key={field}
            name={field}
            type={field === "password" ? "password" : "text"}
            value={form[field as keyof typeof form]}
            onChange={handleChange}
            placeholder={field}
            style={{
              padding: "0.6rem",
              fontSize: "1rem",
              border: "1px solid #ccc",
              borderRadius: "4px",
            }}
          />
        ))}
        <div style={{ display: "flex", gap: "1rem" }}>
          <button
            onClick={handleConnect}
            style={{
              padding: "0.5rem 1rem",
              backgroundColor: "#4CAF50",
              color: "white",
              border: "none",
              borderRadius: "4px",
              cursor: "pointer",
              flex: 1,
            }}
          >
            Подключить
          </button>
          <button
            onClick={onCancel}
            style={{
              padding: "0.5rem 1rem",
              backgroundColor: "#bbb",
              color: "white",
              border: "none",
              borderRadius: "4px",
              cursor: "pointer",
              flex: 1,
            }}
          >
            Отмена
          </button>
        </div>
        {message && (
          <div style={{ marginTop: "0.5rem", color: message.includes("успешно") ? "green" : "red" }}>
            {message}
          </div>
        )}
      </div>
    </div>
  );
}

export default ConnectionForm;
