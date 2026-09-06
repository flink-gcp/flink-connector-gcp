/*
Copyright 2026 The flink-gcp authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
// Read the query and fragment when the selection changes: in-page navigation
// may have changed them since loading. A fallback homepage gets neither.
document.querySelectorAll('.docs-version select').forEach((select) => {
  const current = select.querySelector('option[selected]').value;
  // History can restore the selection that initiated navigation, even though
  // this page still belongs to the version selected in its original HTML.
  window.addEventListener('pageshow', () => {
    select.value = current;
  });
  select.addEventListener('change', () => {
    const option = select.selectedOptions[0];
    const target = new URL(option.value, location.href);
    if (option.hasAttribute('data-same-page')) {
      target.search = location.search;
      target.hash = location.hash;
    }
    location.assign(target.href);
  });
  select.hidden = false;
});
