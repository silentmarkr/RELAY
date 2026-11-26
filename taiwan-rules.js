// taiwan-rules.js (RELAY)
// -----------------------------------------------------------------------------
// Taiwan payroll rules para sa ShiftPay. Ikaw ang nag-e-edit sa admin page (taiwan-rules.html);
// ang app ay kumukuha ng PUBLIC, read-only na JSON sa GET /relay/public/taiwan-rules.
// Ang validation dito ay kapareho ng parseRules() sa ShiftPay (index.html) para hindi ka makapag-save ng file
// na tatanggihan din ng app.
// -----------------------------------------------------------------------------
'use strict';

const RULE_KEYS = ['labIns', 'labRate', 'labEi', 'labEmp', 'helIns', 'helRate', 'helEmp', 'minWage', 'nonLow', 'nonHigh', 'resRate', 'resThresh', 'exOtHrs', 'exCatCap'];
const RULE_PCT = new Set(['labRate', 'labEi', 'labEmp', 'helRate', 'helEmp', 'nonLow', 'nonHigh', 'resRate']);

/** Ibinabalik ang malinis na rules object, o nagtatapon ng Error na may malinaw na mensahe. */
function validate(raw) {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw new Error('Dapat JSON object ang rules.');
    const version = String(raw.version || '').trim().slice(0, 30);
    if (!version) throw new Error('Kailangan ng "version" (hal. 2026.2).');
    if (!raw.values || typeof raw.values !== 'object' || Array.isArray(raw.values)) throw new Error('Kailangan ng "values" section.');
    const out = {
        version,
        effective: String(raw.effective || '').trim().slice(0, 20),
        notes: String(raw.notes || '').trim().slice(0, 300),
        // true = ang ShiftPay ay mag-a-apply nang kusa pagbukas (puwedeng i-off ng user at puwedeng i-undo). false = user ang magpipindot ng Apply.
        autoApply: raw.autoApply === true,
        values: {}
    };
    for (const k of RULE_KEYS) {
        if (raw.values[k] === undefined || raw.values[k] === null || raw.values[k] === '') continue;
        const v = Number(raw.values[k]);
        if (!Number.isFinite(v) || v < 0 || v > 10000000 || (RULE_PCT.has(k) && v > 100)) throw new Error(`Hindi katanggap-tanggap ang halaga ng ${k}: ${raw.values[k]}`);
        out.values[k] = v;
    }
    if (!Object.keys(out.values).length) throw new Error('Walang kahit isang kilalang halaga sa "values".');
    if (raw.grades !== undefined && raw.grades !== null) {
        const g = raw.grades;
        if (!Array.isArray(g) || g.length < 1 || g.length > 60) throw new Error('Ang "grades" ay dapat listahan ng 1 hanggang 60 halaga.');
        const nums = g.map(Number);
        for (let i = 0; i < nums.length; i++) {
            if (!Number.isFinite(nums[i]) || nums[i] <= 0 || (i > 0 && nums[i] <= nums[i - 1])) throw new Error('Ang "grades" ay dapat tumataas na positibong halaga.');
        }
        out.grades = nums;
    }
    return out;
}

/** Tinatanggap ang object o JSON text. */
function parseAndValidate(input) {
    let obj = input;
    if (typeof input === 'string') {
        if (input.length > 100000) throw new Error('Masyadong malaki ang file.');
        try { obj = JSON.parse(input); } catch (e) { throw new Error('Hindi valid na JSON: ' + e.message); }
    }
    return validate(obj);
}

module.exports = { RULE_KEYS, validate, parseAndValidate };
