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
      setMessage("Подключение успешно сохранено");
      onSave();
    }
  } catch {
    setMessage("Ошибка запроса");
  }
  };



  return (
    <div style={{ marginTop: "20px" }}>
      <h3>{existing ? "Редактировать подключение" : "Новое подключение"}</h3>
      <div style={{ display: "grid", gap: "10px", maxWidth: "400px" }}>
        {["name", "host", "port", "dbname", "user", "password"].map((field) => (
          <input
            key={field}
            name={field}
            type={field === "password" ? "password" : "text"}
            value={form[field as keyof typeof form]}
            onChange={handleChange}
            placeholder={field}
          />
        ))}
        <button onClick={handleConnect}>Подключить</button>
        <button onClick={onCancel}>Отмена</button>
        <div>{message}</div>
      </div>
    </div>
  );
}

export default ConnectionForm;
