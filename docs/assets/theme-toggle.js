// Copyright 2026 The flink-gcp authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Colour-scheme selection. Inlined into <head> by the inject/head partial so
// that it runs before first paint: setting the attribute afterwards would show
// the reader a light page for one frame before repainting it dark.
//
// The attribute is written on every load, resolving "follow the OS" to a
// concrete value, so the stylesheet needs rules for two states rather than
// three. A reader without JavaScript gets no attribute at all and keeps the
// media-query behaviour the site had before this file existed; the button is
// hidden from them, because it is the only control here that cannot work.
(function () {
  var KEY = "book.theme";
  var DARK = "(prefers-color-scheme: dark)";
  var root = document.documentElement;

  // localStorage is not always available: private modes and per-site storage
  // settings make it *throw* on access rather than return null. Catching that
  // keeps the script — and the button with it — alive, but catching alone is
  // not enough: with nothing written, every navigation re-resolves the OS
  // preference and the reader's choice is gone one link later. Measured, in a
  // browser where storage is blocked; it looks exactly like the toggle not
  // working. So a cookie is the fallback, which is a different mechanism with
  // a different switch behind it rather than a second attempt at the same one.
  //
  // Read in the same order it is written, so a reader who once stored a choice
  // does not have a stale cookie override it.
  function stored() {
    var value = null;
    try {
      value = localStorage.getItem(KEY);
    } catch (error) {
      value = null;
    }
    if (value !== "light" && value !== "dark") {
      var match = document.cookie.match(/(?:^|;\s*)book\.theme=(light|dark)(?:;|$)/);
      value = match ? match[1] : null;
    }
    return value === "light" || value === "dark" ? value : null;
  }

  function remember(theme) {
    var persisted = false;
    try {
      localStorage.setItem(KEY, theme);
      persisted = true;
    } catch (error) {
      persisted = false;
    }
    if (!persisted) {
      try {
        // Session-length and same-site: this is a display preference, so it
        // needs neither an expiry the reader cannot see nor cross-site travel.
        document.cookie = KEY + "=" + theme + ";path=/;SameSite=Lax";
      } catch (error) {
        // A choice that cannot be persisted still applies to this page.
      }
    }
  }

  function preferred() {
    return window.matchMedia && window.matchMedia(DARK).matches
      ? "dark"
      : "light";
  }

  root.setAttribute("data-theme", stored() || preferred());
  // Marks that the button below has a working handler. The stylesheet hides it
  // until this class appears.
  root.classList.add("book-js");

  // Until the reader chooses, the site still follows the OS switching under it
  // while the page stays open — which is what it did before, and what dropping
  // the media query in favour of a one-shot resolve would have cost.
  if (window.matchMedia) {
    var query = window.matchMedia(DARK);
    var follow = function () {
      if (!stored()) {
        root.setAttribute("data-theme", preferred());
      }
    };
    if (query.addEventListener) {
      query.addEventListener("change", follow);
    } else if (query.addListener) {
      query.addListener(follow);
    }
  }

  // Delegated, so this runs in <head> without waiting for the button to exist.
  document.addEventListener("click", function (event) {
    var target = event.target;
    if (!target || !target.closest || !target.closest(".book-theme-toggle")) {
      return;
    }
    var next = root.getAttribute("data-theme") === "dark" ? "light" : "dark";
    root.setAttribute("data-theme", next);
    remember(next);
  });
})();
