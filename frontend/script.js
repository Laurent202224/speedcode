const messagesEl = document.querySelector("#messages");
const formEl = document.querySelector("#chatForm");
const inputEl = document.querySelector("#promptInput");
const latitudeEl = document.querySelector("#latitudeInput");
const longitudeEl = document.querySelector("#longitudeInput");
const sendButton = document.querySelector("#sendButton");
const clearButton = document.querySelector("#clearButton");
const newChatButton = document.querySelector("#newChatButton");
const menuButton = document.querySelector("#menuButton");
const locationGridEl = document.querySelector("#locationGrid");
const agentStatusEl = document.querySelector("#agentStatus");
const suggestionButtons = document.querySelectorAll("[data-prompt]");

const storageKey = "hospital-matcher.full-pipeline.messages";
let appConfig = { test_mode: false };

const starterMessages = [
  {
    role: "assistant",
    content:
      "Describe symptoms or a diagnosis and include a location. I will extract the care need, rank nearby providers, and show trust and Google review signals.",
  },
];

let messages = loadMessages();
let isThinking = false;

initialize();

formEl.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (isThinking) {
    return;
  }

  const prompt = inputEl.value.trim();
  if (!prompt) {
    addMessage("assistant", "Please describe the symptoms, diagnosis, or care need.");
    return;
  }

  let latitude = null;
  let longitude = null;
  if (appConfig.test_mode) {
    latitude = Number.parseFloat(latitudeEl.value);
    longitude = Number.parseFloat(longitudeEl.value);
    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
      addMessage("assistant", "Please enter a valid latitude and longitude.");
      return;
    }
  }

  addMessage("user", formatUserMessage(prompt, latitude, longitude));
  inputEl.value = "";
  resizeInput();
  setThinking(true);

  try {
    const result = await fetchRecommendation(prompt, latitude, longitude);
    removeTypingMessage();
    addMessage("assistant", formatRecommendation(result), { result });
  } catch (error) {
    removeTypingMessage();
    addMessage("assistant", error.message);
  } finally {
    setThinking(false);
  }
});

inputEl.addEventListener("input", resizeInput);

inputEl.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    formEl.requestSubmit();
  }
});

clearButton.addEventListener("click", resetChat);
newChatButton.addEventListener("click", resetChat);
menuButton.addEventListener("click", () => {
  document.body.classList.toggle("sidebar-open");
});

for (const button of suggestionButtons) {
  button.addEventListener("click", () => {
    inputEl.value = button.dataset.prompt || "";
    resizeInput();
    inputEl.focus();
  });
}

document.addEventListener("click", (event) => {
  if (
    document.body.classList.contains("sidebar-open") &&
    !event.target.closest(".sidebar") &&
    !event.target.closest("#menuButton")
  ) {
    document.body.classList.remove("sidebar-open");
  }
});

async function initialize() {
  appConfig = await fetchAppConfig();
  applyAppMode();
  renderMessages();
  resizeInput();
}

async function fetchAppConfig() {
  try {
    const response = await fetch("/api/app-config");
    if (!response.ok) {
      return { test_mode: false };
    }
    return await response.json();
  } catch {
    return { test_mode: false };
  }
}

function applyAppMode() {
  if (appConfig.test_mode) {
    starterMessages[0].content =
      "Enter a supported diagnosis plus latitude and longitude. I will rank the closest matching providers with trust and Google review signals.";
    locationGridEl.hidden = false;
    inputEl.placeholder = "Enter a supported diagnosis, for example: Dentistry";
    agentStatusEl.textContent = "Test mode";
    return;
  }

  starterMessages[0].content =
    "Describe symptoms or a diagnosis and include a location. I will extract the care need and rank nearby providers.";
  locationGridEl.hidden = true;
  inputEl.placeholder =
    "Describe symptoms and location, for example: severe toothache near Connaught Place, Delhi";
  agentStatusEl.textContent = "Full pipeline";
}

function loadMessages() {
  try {
    const stored = JSON.parse(localStorage.getItem(storageKey));
    if (Array.isArray(stored) && stored.length > 0) {
      return stored;
    }
  } catch {
    localStorage.removeItem(storageKey);
  }

  return [...starterMessages];
}

function saveMessages() {
  localStorage.setItem(storageKey, JSON.stringify(messages));
}

function addMessage(role, content, options = {}) {
  messages.push({
    role,
    content,
    typing: Boolean(options.typing),
    result: options.result || null,
  });
  renderMessages();
  if (!options.typing) {
    saveMessages();
  }
}

function renderMessages() {
  messagesEl.innerHTML = "";

  for (const message of messages) {
    const row = document.createElement("article");
    row.className = `message-row ${message.role}`;

    if (message.role === "assistant") {
      row.append(createAvatar());
    }

    const bubble = document.createElement("div");
    bubble.className = "message";

    if (message.typing) {
      bubble.innerHTML =
        '<div class="typing" aria-label="Agent is typing"><span></span><span></span><span></span></div>';
    } else if (message.role === "assistant" && shouldRenderResult(message.result)) {
      bubble.append(createResultCard(message.result, message.content));
    } else {
      const paragraph = document.createElement("p");
      paragraph.textContent = message.content;
      bubble.append(paragraph);
    }

    row.append(bubble);
    messagesEl.append(row);
  }

  messagesEl.scrollTop = messagesEl.scrollHeight;
}

function createAvatar() {
  const avatar = document.createElement("div");
  avatar.className = "avatar";
  avatar.textContent = "AI";
  avatar.setAttribute("aria-hidden", "true");
  return avatar;
}

function shouldRenderResult(result) {
  return Boolean(result?.diagnosis || (Array.isArray(result?.matches) && result.matches.length > 0));
}

function createResultCard(result, fallbackMessage) {
  const card = document.createElement("div");
  card.className = "result-card";

  const diagnosis = result.diagnosis || {};
  const matches = Array.isArray(result.matches) ? result.matches : [];
  const extraction = result.extraction || {};
  const bestMatch = result.best_match || null;

  const header = document.createElement("div");
  header.className = "result-header";
  const titleWrap = document.createElement("div");
  const eyebrow = document.createElement("p");
  eyebrow.className = "eyebrow";
  eyebrow.textContent = result.test_mode ? "Test-mode route" : "Pipeline route";
  const title = document.createElement("h2");
  title.className = "result-title";
  title.textContent = diagnosis.name || extraction.diagnosis_name || "Provider match";
  titleWrap.append(eyebrow, title);

  const badges = document.createElement("div");
  badges.className = "badge-row";
  const confidence = formatConfidence(diagnosis.confidence_score);
  if (confidence) {
    badges.append(createBadge(`${confidence} confidence`));
  }
  if (result.rerank?.match_strength) {
    badges.append(createBadge(`${result.rerank.match_strength} match`));
  }
  header.append(titleWrap, badges);
  card.append(header);

  const summary = document.createElement("div");
  summary.className = "summary-grid";
  summary.append(
    createSummaryItem("Location", formatResultLocation(result)),
  );
  card.append(summary);

  if (extraction.need_description) {
    const need = document.createElement("p");
    need.className = "next-step";
    need.textContent = extraction.need_description;
    card.append(need);
  }

  if (bestMatch) {
    card.append(createSelectedHospitalCard(bestMatch, result.rerank || {}));
  }

  if (matches.length > 0) {
    const list = document.createElement("div");
    list.className = "provider-list";
    for (const [index, provider] of matches.slice(0, 5).entries()) {
      list.append(createProviderCard(provider, index));
    }
    card.append(list);
  } else {
    const empty = document.createElement("p");
    empty.className = "next-step";
    empty.textContent = fallbackMessage || "No matching provider was found.";
    card.append(empty);
  }

  return card;
}

function createSelectedHospitalCard(provider, rerank) {
  const card = document.createElement("section");
  card.className = "selected-provider";

  const header = document.createElement("div");
  header.className = "selected-provider-header";
  const titleWrap = document.createElement("div");
  const eyebrow = document.createElement("p");
  eyebrow.className = "eyebrow";
  eyebrow.textContent = "Best pick";
  const title = document.createElement("h3");
  title.className = "selected-provider-title";
  title.textContent = provider.name || "Selected provider";
  titleWrap.append(eyebrow, title);
  header.append(titleWrap);

  const distance = formatDistance(provider.distance_km);
  if (distance) {
    header.append(createBadge(distance));
  }
  card.append(header);

  const metrics = createProviderMetrics(provider);
  if (metrics) {
    card.append(metrics);
  }

  const source = provider.source_context || {};
  const details = [
    source.address ? ["Location", source.address] : null,
    provider.selection_reason || rerank.reason ? ["Why", provider.selection_reason || rerank.reason] : null,
    source.officialPhone ? ["Phone", source.officialPhone] : null,
  ].filter(Boolean);

  if (details.length > 0) {
    const list = document.createElement("dl");
    list.className = "selected-details";
    for (const [label, value] of details) {
      const term = document.createElement("dt");
      term.textContent = label;
      const description = document.createElement("dd");
      description.textContent = value;
      list.append(term, description);
    }
    card.append(list);
  }

  const websiteUrl = normalizeUrl(source.officialWebsite || provider.website_url || provider.officialWebsite);
  if (websiteUrl) {
    const actions = document.createElement("div");
    actions.className = "provider-actions";
    actions.append(createExternalLink("Website", websiteUrl));
    card.append(actions);
  }

  return card;
}

function createProviderCard(provider, index) {
  const websiteUrl = normalizeUrl(provider.source_context?.officialWebsite || provider.website_url || provider.officialWebsite);
  const card = document.createElement(websiteUrl ? "a" : "div");
  card.className = "provider-card";
  if (websiteUrl) {
    card.href = websiteUrl;
    card.target = "_blank";
    card.rel = "noopener noreferrer";
  }

  const rank = document.createElement("div");
  rank.className = "provider-rank";
  rank.textContent = String(index + 1);

  const details = document.createElement("div");
  const name = document.createElement("p");
  name.className = "provider-name";
  name.textContent = provider.name || "Unnamed provider";
  const meta = document.createElement("p");
  meta.className = "provider-meta";
  meta.textContent = provider.type || provider.diagnosis || "Healthcare provider";
  details.append(name, meta);
  const metrics = createProviderMetrics(provider);
  if (metrics) {
    details.append(metrics);
  }

  const distance = document.createElement("div");
  distance.className = "provider-distance";
  distance.textContent = formatDistance(provider.distance_km) || "Distance unavailable";

  card.append(rank, details, distance);
  return card;
}

function createProviderMetrics(provider) {
  const items = [
    formatTrustScore(provider.trustworthy_score),
    formatGoogleRating(provider),
    provider.consistency ? `Consistency ${provider.consistency}` : "",
  ].filter(Boolean);

  if (items.length === 0) {
    return null;
  }

  const metrics = document.createElement("div");
  metrics.className = "provider-metrics";
  for (const item of items) {
    const pill = document.createElement("span");
    pill.textContent = item;
    metrics.append(pill);
  }
  return metrics;
}

function createBadge(text, className = "") {
  const badge = document.createElement("span");
  badge.className = ["badge", className].filter(Boolean).join(" ");
  badge.textContent = text;
  return badge;
}

function createSummaryItem(label, value) {
  const item = document.createElement("div");
  item.className = "summary-item";
  const labelEl = document.createElement("span");
  labelEl.textContent = label;
  const valueEl = document.createElement("strong");
  valueEl.textContent = value || "Unavailable";
  item.append(labelEl, valueEl);
  return item;
}

function createExternalLink(label, url) {
  const link = document.createElement("a");
  link.className = "provider-action";
  link.href = url;
  link.target = "_blank";
  link.rel = "noopener noreferrer";
  link.textContent = label;
  return link;
}

function formatUserMessage(prompt, latitude, longitude) {
  if (!appConfig.test_mode) {
    return prompt;
  }
  return `${prompt}\nLatitude: ${latitude}\nLongitude: ${longitude}`;
}

async function fetchRecommendation(query, latitude, longitude) {
  let response;
  try {
    response = await fetch("/api/recommend", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        query,
        latitude,
        longitude,
        limit: 5,
      }),
    });
  } catch {
    throw new Error("Backend not reachable. Start the app with: python3 app/server.py");
  }

  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "The backend could not process that request.");
  }
  return payload;
}

function formatRecommendation(result) {
  if (result.message) {
    return result.message;
  }
  const diagnosis = result.diagnosis?.name || result.extraction?.diagnosis_name || "Unknown";
  const matches = Array.isArray(result.matches) ? result.matches : [];
  return `${diagnosis}\n${matches.length} provider matches returned.`;
}

function setThinking(nextValue) {
  isThinking = nextValue;
  sendButton.disabled = nextValue;

  if (nextValue) {
    addMessage("assistant", "", { typing: true });
  }
}

function removeTypingMessage() {
  messages = messages.filter((message) => !message.typing);
  renderMessages();
}

function resetChat() {
  messages = [...starterMessages];
  saveMessages();
  renderMessages();
  document.body.classList.remove("sidebar-open");
  inputEl.focus();
}

function resizeInput() {
  inputEl.style.height = "auto";
  inputEl.style.height = `${Math.min(inputEl.scrollHeight, 180)}px`;
}

function normalizeUrl(value) {
  if (!value || typeof value !== "string") {
    return "";
  }
  const url = value.trim();
  if (!url || url === "null") {
    return "";
  }
  return /^https?:\/\//i.test(url) ? url : `https://${url}`;
}

function formatResultLocation(result) {
  const extraction = result.extraction || {};
  if (extraction.location_text) {
    return extraction.location_text;
  }
  const input = result.input || {};
  if (Number.isFinite(input.latitude) && Number.isFinite(input.longitude)) {
    return `${input.latitude.toFixed(4)}, ${input.longitude.toFixed(4)}`;
  }
  return "";
}

function formatConfidence(value) {
  if (!Number.isFinite(value)) {
    return "";
  }
  const percentage = value <= 1 ? value * 100 : value;
  return `${Math.round(percentage)}%`;
}

function formatDistance(value) {
  return Number.isFinite(value) ? `${value.toFixed(2)} km` : "";
}

function formatTrustScore(value) {
  if (!Number.isFinite(value)) {
    return "";
  }
  return `Trust ${value.toFixed(1)}/10`;
}

function formatGoogleRating(provider) {
  if (!Number.isFinite(provider.google_rating)) {
    return "";
  }
  const count = Number.isFinite(provider.google_rating_count)
    ? ` ${provider.google_rating_count} reviews`
    : "";
  return `Google ${provider.google_rating.toFixed(1)}/5${count}`;
}
