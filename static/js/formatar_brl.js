function formatarBRL(textoOriginal) {
  if (!textoOriginal.includes("R$")) return null;

  let texto = textoOriginal.replace(/[R$\s]/g, "");

  if (texto.includes(",") && texto.includes(".")) {
    if (texto.indexOf(",") > texto.indexOf(".")) {
      texto = texto.replace(/\./g, "").replace(",", ".");
    } else {
      texto = texto.replace(/,/g, "");
    }
  } else if (texto.includes(",")) {
    texto = texto.replace(",", ".");
  } else {
    texto = texto.replace(/,/g, "");
  }

  const num = parseFloat(texto);
  if (isNaN(num)) return null;

  return num.toLocaleString("pt-BR", {
    style: "currency",
    currency: "BRL"
  });
}

function aplicarFormatacaoBRL() {
  document.querySelectorAll("p.valor, td, h3, h4, span").forEach(el => {
    const texto = el.innerText.trim();
    if (!texto.startsWith("R$")) return;
    const novo = formatarBRL(texto);
    if (novo) el.innerText = novo;
  });
}

document.addEventListener("DOMContentLoaded", () => {
  aplicarFormatacaoBRL();
  setTimeout(aplicarFormatacaoBRL, 800);
  setTimeout(aplicarFormatacaoBRL, 2000);
});
