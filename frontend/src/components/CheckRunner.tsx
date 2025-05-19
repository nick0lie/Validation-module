import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";

function CheckRunner() {
  const [selectedConnection, setSelectedConnection] = useState<string | null>(null);
  const [rules, setRules] = useState<any[]>([]);
  const [selectedRule, setSelectedRule] = useState("");
  const [tables, setTables] = useState<string[]>([]);
  const [sourceColumns, setSourceColumns] = useState<string[]>([]);
  const [targetColumns, setTargetColumns] = useState<string[]>([]);
  const [tableParams, setTableParams] = useState<{ [key: string]: string }>({});
  const [message, setMessage] = useState("");
  const [reportRows, setReportRows] = useState<any[] | null>(null);
  const navigate = useNavigate();

  useEffect(() => {
    const stored = localStorage.getItem("selectedConnection");
    if (!stored) setMessage("Подключение не выбрано");
    setSelectedConnection(stored);
  }, []);

  useEffect(() => {
    fetch("http://localhost:8000/api/rules/")
      .then((res) => res.json())
      .then(setRules)
      .catch(() => setMessage("Ошибка загрузки проверок"));
  }, []);

  useEffect(() => {
    if (selectedConnection) {
      fetch(`http://localhost:8000/api/tables?connection_name=${selectedConnection}`)
        .then((res) => res.json())
        .then(setTables)
        .catch(() => setMessage("Ошибка загрузки таблиц"));
    }
  }, [selectedRule, selectedConnection]);

  const fetchColumns = async (table: string, role: "source" | "target") => {
    if (!selectedConnection) return;
    try {
      const res = await fetch(`http://localhost:8000/api/columns?connection_name=${selectedConnection}&table=${table}`);
      const data = await res.json();
      if (role === "source") setSourceColumns(data);
      else setTargetColumns(data);
    } catch (e) {
      console.error("Ошибка загрузки колонок", e);
    }
  };

  const handleRunCheck = async () => {
    setMessage("Выполняется проверка...");
    setReportRows(null);
    try {
      const response = await fetch("http://localhost:8000/api/trigger_dag", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          rule_name: selectedRule,
          connection_name: selectedConnection,
          table_params: tableParams,
        }),
      });

      const data = await response.json();

      if (response.ok) {
        setMessage("Проверка запущена. Ожидание результата...");
        await pollForReport();
      } else {
        setMessage(`Ошибка: ${JSON.stringify(data.detail)}`);
      }
    } catch (error: any) {
      setMessage("Ошибка при запуске проверки: " + error.message);
    }
  };

  const pollForReport = async () => {
    let retries = 0;
    while (retries < 30) {
      try {
        const res = await fetch("http://localhost:8000/api/report/last");
        if (res.ok) {
          const report = await res.json();
          setMessage("Проверка завершена. Отчет загружен.");
          setReportRows(report.rows);
          return;
        }
      } catch (_) {}
      await new Promise((res) => setTimeout(res, 2000));
      retries++;
    }
    setMessage("Ошибка: отчет не найден после ожидания.");
  };

  return (
    <div style={{ padding: "2rem" }}>
      <h2>Выполнение проверки</h2>
      <p><strong>Подключение:</strong> {selectedConnection || "не выбрано"}</p>
      <button onClick={() => {
        localStorage.removeItem("selectedConnection");
        navigate("/");
      }}>🔁 Сменить подключение</button>

      <div style={{ marginTop: "1rem" }}>
        <select
          value={selectedRule}
          onChange={(e) => setSelectedRule(e.target.value)}
        >
          <option value="">Выберите правило</option>
          {rules.map((r) => <option key={r.rule_id} value={r.rule_name}>{r.rule_name}</option>)}
        </select>
      </div>

      {selectedRule === "referential_integrity_check" && (
        <div style={{ marginTop: "1rem" }}>
          <label>Исходная таблица:</label>
          <select value={tableParams.source_table || ""} onChange={(e) => {
            const value = e.target.value;
            setTableParams(prev => ({ ...prev, source_table: value }));
            fetchColumns(value, "source");
          }}>
            <option value="">Выберите</option>
            {tables.map(t => <option key={t} value={t}>{t}</option>)}
          </select>

          <label>Поле из исходной таблицы:</label>
          <select value={tableParams.source_column || ""} onChange={(e) => setTableParams(prev => ({ ...prev, source_column: e.target.value }))}>
            <option value="">Выберите</option>
            {sourceColumns.map(c => <option key={c} value={c}>{c}</option>)}
          </select>

          <label>Целевая таблица:</label>
          <select value={tableParams.target_table || ""} onChange={(e) => {
            const value = e.target.value;
            setTableParams(prev => ({ ...prev, target_table: value }));
            fetchColumns(value, "target");
          }}>
            <option value="">Выберите</option>
            {tables.map(t => <option key={t} value={t}>{t}</option>)}
          </select>

          <label>Поле из целевой таблицы:</label>
          <select value={tableParams.target_column || ""} onChange={(e) => setTableParams(prev => ({ ...prev, target_column: e.target.value }))}>
            <option value="">Выберите</option>
            {targetColumns.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>
      )}

      <button style={{ marginTop: "1rem" }} onClick={handleRunCheck}>🚀 Запустить проверку</button>

      {message && <p style={{ color: message.startsWith("Ошибка") ? "red" : "green" }}>{message}</p>}

      {reportRows && (
        <div style={{ marginTop: "1rem" }}>
          <h4>Результаты:</h4>
          <table border={1} cellPadding={5}>
            <thead>
              <tr>{Object.keys(reportRows[0] || {}).map(col => <th key={col}>{col}</th>)}</tr>
            </thead>
            <tbody>
              {reportRows.map((row, idx) => (
                <tr key={idx}>
                  {Object.values(row).map((val, i) => <td key={i}>{val}</td>)}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export default CheckRunner;
