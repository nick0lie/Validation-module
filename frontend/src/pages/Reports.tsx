import { useState, useEffect } from "react";

function Reports() {
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [availableRules, setAvailableRules] = useState<string[]>([]);
  const [selectedRules, setSelectedRules] = useState<string[]>([]);
  const [selectAll, setSelectAll] = useState(false);
  const [message, setMessage] = useState("");
  const [downloading, setDownloading] = useState(false);

  useEffect(() => {
    if (startDate && endDate) {
      fetch(`/api/available_rules?start_date=${startDate}&end_date=${endDate}`)
        .then(res => res.json())
        .then(data => {
          setAvailableRules(data);
          setSelectedRules(data);
          setSelectAll(true);
        })
        .catch(() => setMessage("Ошибка загрузки списка проверок"));
    }
  }, [startDate, endDate]);

  const toggleSelectAll = () => {
    if (selectAll) {
      setSelectedRules([]);
      setSelectAll(false);
    } else {
      setSelectedRules(availableRules);
      setSelectAll(true);
    }
  };

  const handleExport = async () => {
    setMessage("Запуск экспорта...");
    setDownloading(true);

    const res = await fetch("/api/export/period", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        start_date: startDate,
        end_date: endDate,
        rule_names: selectedRules
      }),
    });

    if (!res.ok) {
      setMessage("Ошибка запуска экспорта");
      setDownloading(false);
      return;
    }

    const { filename } = await res.json();
    setMessage("Ожидание файла...");

    const poll = async () => {
      for (let i = 0; i < 20; i++) {
        await new Promise(r => setTimeout(r, 3000));
        const resp = await fetch(`/api/export/period/download?start_date=${startDate}&end_date=${endDate}`);
        if (resp.ok) {
          const blob = await resp.blob();
          const url = window.URL.createObjectURL(blob);
          const link = document.createElement("a");
          link.href = url;
          link.download = filename || "report.xlsx";
          document.body.appendChild(link);
          link.click();
          link.remove();
          setMessage("✅ Отчет загружен");
          setDownloading(false);
          return;
        }
      }
      setMessage("Время ожидания истекло");
      setDownloading(false);
    };

    poll();
  };

  return (
    <div style={{ backgroundColor: "#dae6f2", minHeight: "100vh", padding: "2rem", fontFamily: "sans-serif" }}>
      <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: "1rem" }}>
        <a href="/" style={{ backgroundColor: "#4CAF50", color: "white", padding: "0.5rem 1rem", borderRadius: "4px", textDecoration: "none" }}>
          Подключения
        </a>
      </div>

      <div style={{
        maxWidth: 800,
        margin: "0 auto",
        padding: "2rem",
        backgroundColor: "white",
        borderRadius: "8px",
        boxShadow: "0 0 10px rgba(0, 0, 0, 0.1)"
      }}>
        <h2 style={{ fontSize: "1.6rem", marginBottom: "0.5rem" }}>Экспорт отчётов</h2>
        <p style={{ marginBottom: "1.5rem" }}>Выберите диапазон дат и нужные проверки для выгрузки результатов.</p>

        <div style={{ marginBottom: "1rem" }}>
          <label>Дата от:</label><br />
          <input type="date" value={startDate} onChange={e => setStartDate(e.target.value)} />
        </div>

        <div style={{ marginBottom: "1rem" }}>
          <label>Дата до:</label><br />
          <input type="date" value={endDate} onChange={e => setEndDate(e.target.value)} />
        </div>

        {availableRules.length > 0 && (
          <div style={{ marginBottom: "1rem" }}>
            <label><strong>Выберите проверки:</strong></label><br />
            <label style={{ display: "block", marginBottom: "0.5rem" }}>
              <input
                type="checkbox"
                checked={selectAll}
                onChange={toggleSelectAll}
              /> Выбрать все
            </label>
            {availableRules.map(rule => (
              <label key={rule} style={{ display: "block" }}>
                <input
                  type="checkbox"
                  value={rule}
                  checked={selectedRules.includes(rule)}
                  onChange={e => {
                    const checked = e.target.checked;
                    setSelectedRules(prev =>
                      checked ? [...prev, rule] : prev.filter(r => r !== rule)
                    );
                    setSelectAll(false);
                  }}
                />
                {" "}{rule}
              </label>
            ))}
          </div>
        )}

        <button
          onClick={handleExport}
          disabled={downloading || selectedRules.length === 0}
          style={{
            padding: "0.6rem 1.2rem",
            fontSize: "1rem",
            backgroundColor: "#4CAF50",
            color: "white",
            border: "none",
            borderRadius: "4px",
            cursor: "pointer"
          }}
        >
          📥 Скачать отчет
        </button>

        {message && <p style={{ marginTop: "1rem", color: message.includes("Ошибка") ? "red" : "green" }}>{message}</p>}
      </div>
    </div>
  );
}

export default Reports;
