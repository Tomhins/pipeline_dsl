const vscode = require('vscode');
const path = require('path');

// ---------------------------------------------------------------------------
// Command documentation — used by the hover provider
// ---------------------------------------------------------------------------
const COMMAND_DOCS = {
    source:    '`source "file.csv"`\n\nLoad a CSV, JSON, or Parquet file into the pipeline.\n\nOptional chunked streaming for large files:\n`source "file.csv" chunk 100000`\n\nRow-safe operations are applied per chunk to reduce peak memory usage.',
    save:      '`save "file.csv|json|parquet"`\n\nWrite the current DataFrame to a CSV, JSON, or Parquet file. Output directories are created automatically.',
    merge:     '`merge "file.csv"`\n\nAppend rows from another CSV file (union / stack).',
    join:      '`join "file.csv" on <column> [inner|left|right|outer]`\n\nJoin with another CSV on a key column. Type defaults to `inner`.\n\n| Type | Behaviour |\n|---|---|\n| `inner` | Only matching rows (default) |\n| `left` | All left rows; nulls for unmatched right |\n| `right` | All right rows; nulls for unmatched left |\n| `outer` | All rows from both sides |\n\nExample: `join "data/depts.csv" on dept_id left`',
    foreach:   '`foreach "glob/pattern/*.csv"`\n\nLoad and concatenate all CSVs matching a glob pattern.',
    include:   '`include "file.ppl"`\n\nInclude and execute another pipeline file inline.',
    filter:    '`filter <col> <op> <value>`\n\nFilter rows by a condition. Supports `and`/`or` on one line.\n\nOperators: `>` `<` `>=` `<=` `==` `!=`',
    where:     '`where <col> <op> <value>`\n\nSQL-friendly alias for `filter`.',
    select:    '`select col1, col2, ...`\n\nKeep only the listed columns.',
    group:     '`group by col1, col2, ...`\n\nGroup the data by one or more columns for subsequent aggregation.',
    count:     '`count` or `count if <col> <op> <value>`\n\nCount rows in the current dataset (or matching a condition) without filtering.',
    sort:      '`sort by col [asc|desc], ...`\n\nSort rows by one or more columns.',
    rename:    '`rename <old> <new>`\n\nRename a column.',
    add:       '`add <col> = <expression>`\n\nAdd a computed column. Supports arithmetic and `if … then … else …`.\n\nExample: `add tax = salary * 0.2`\nExample: `add band = if salary > 100000 then "senior" else "junior"`',
    drop:      '`drop col1, col2, ...`\n\nRemove one or more columns.',
    limit:     '`limit <n>`\n\nKeep only the first *n* rows.',
    distinct:  '`distinct`\n\nRemove duplicate rows.',
    sample:    '`sample <n>` or `sample <n>%`\n\nTake a random sample of *n* rows or *n*% of the data.',
    trim:      '`trim <column>`\n\nStrip leading/trailing whitespace from a string column.',
    uppercase: '`uppercase <column>`\n\nConvert a string column to uppercase.',
    lowercase: '`lowercase <column>`\n\nConvert a string column to lowercase.',
    cast:      '`cast <column> <type>`\n\nCast a column to a different type.\n\nTypes: `int` `float` `str` `datetime` `bool`',
    replace:   '`replace <col> <old> <new>`\n\nReplace occurrences of a value in a column.',
    pivot:     '`pivot index=<col> column=<col> value=<col>`\n\nReshape data from long to wide format.',
    sum:       '`sum <column>`\n\nSum a numeric column (per group or total).',
    avg:       '`avg <column>`\n\nAverage of a numeric column (per group or total).',
    min:       '`min <column>`\n\nMinimum value of a column (per group or total).',
    max:       '`max <column>`\n\nMaximum value of a column (per group or total).',
    agg:       '`agg sum <col>, avg <col>, count`\n\nMultiple aggregations at once after a `group by`.\n\nVerbs: `sum` `avg` `min` `max` `count`',
    print:     '`print`\n\nPrint the current DataFrame to stdout.',
    schema:    '`schema`\n\nPrint column names and data types.',
    inspect:   '`inspect`\n\nPrint column names, types, null counts, and unique counts.',
    head:      '`head <n>`\n\nPrint the first *n* rows to stdout (does not modify pipeline data).',
    log:       '`log <message>`\n\nPrint a message to the terminal during execution.',
    assert:    '`assert <col> <op> <value>`\n\nFail the pipeline if any row violates the condition.',
    fill:      '`fill <column> <strategy|value>`\n\nFill missing values.\n\nStrategies: `mean` `median` `mode` `forward` `backward` `drop`\nOr supply a literal value: `fill country "Unknown"`',
    set:       '`set <name> = <value>`\n\nSet a named variable referenceable as `$name` in other commands.\n\nSpecial: `set sandbox = <dir>` restricts all file I/O to that directory tree.',
    env:       '`env <VAR_NAME>`\n\nLoad an OS environment variable into the pipeline variable store.',
    try:       '`try` / `on_error <action>`\n\nWrap commands in a `try` block. If any command fails, the `on_error` handler runs instead of stopping the pipeline.\n\nActions: `skip` | `log "message"` | any pipeline command\n\n```\ntry\n    assert salary > 0\non_error fill salary 0\n```',
    on_error:  '`on_error <action>`\n\nError handler for a `try` block. Actions: `skip`, `log "message"`, or any pipeline command.',
    skip:      'Used with `on_error skip` to silently swallow errors from a `try` block.',
    chunk:     'Used with `source` to enable chunked streaming: `source "file.csv" chunk 100000`',
    inner:     'Join type: only rows with matching keys in both files (default for `join`).',
    left:      'Join type: all left rows; nulls for unmatched right rows.',
    right:     'Join type: all right rows; nulls for unmatched left rows.',
    outer:     'Join type: all rows from both sides; nulls where a match is missing.',
    // modifiers / keywords
    by:        'Used with `group by` and `sort by`.',
    on:        'Used with `join … on <column>`.',
    asc:       'Sort direction: ascending (default).',
    desc:      'Sort direction: descending.',
    and:       'Logical AND for compound `filter`/`where` conditions.',
    or:        'Logical OR for compound `filter`/`where` conditions.',
    if:        'Used in `add <col> = if <cond> then <val> else <val>`.',
    then:      'Used in `add <col> = if <cond> then <val> else <val>`.',
    else:      'Used in `add <col> = if <cond> then <val> else <val>`.',
    mean:      'Fill strategy: replace nulls with the column mean.',
    median:    'Fill strategy: replace nulls with the column median.',
    mode:      'Fill strategy: replace nulls with the column mode.',
    forward:   'Fill strategy: forward-fill (propagate last valid value).',
    backward:  'Fill strategy: backward-fill.',
    int:       'Cast type: integer.',
    float:     'Cast type: floating-point number.',
    str:       'Cast type: string.',
    datetime:  'Cast type: datetime.',
    bool:      'Cast type: boolean.',
};

/**
 * Called when the extension is activated (first time a .ppl file is opened).
 * @param {vscode.ExtensionContext} context
 */
function activate(context) {
    const runCmd = vscode.commands.registerCommand('ppl.runPipeline', () => {
        const editor = vscode.window.activeTextEditor;

        if (!editor) {
            vscode.window.showErrorMessage('Pipeline DSL: No active file.');
            return;
        }

        const filePath = editor.document.uri.fsPath;

        if (!filePath.endsWith('.ppl')) {
            vscode.window.showErrorMessage('Pipeline DSL: Active file is not a .ppl file.');
            return;
        }

        // Save the file first so the runner always sees the latest version
        editor.document.save().then(() => {
            // Reuse an existing "Pipeline DSL" terminal or create one
            let terminal = vscode.window.terminals.find(t => t.name === 'Pipeline DSL');
            if (!terminal) {
                terminal = vscode.window.createTerminal({
                    name: 'Pipeline DSL',
                    cwd: path.dirname(filePath)
                });
            }

            terminal.show(true); // true = don't steal focus from editor
            terminal.sendText(`ppl "${filePath}"`);
        });
    });

    context.subscriptions.push(runCmd);

    // -----------------------------------------------------------------------
    // Hover provider — show docs when hovering over a keyword
    // -----------------------------------------------------------------------
    const hoverProvider = vscode.languages.registerHoverProvider('ppl', {
        provideHover(document, position) {
            const range = document.getWordRangeAtPosition(position, /[a-zA-Z_]\w*/);
            if (!range) return;
            const word = document.getText(range).toLowerCase();
            const doc = COMMAND_DOCS[word];
            if (!doc) return;
            const md = new vscode.MarkdownString(doc);
            md.isTrusted = true;
            return new vscode.Hover(md, range);
        }
    });
    context.subscriptions.push(hoverProvider);

    // -----------------------------------------------------------------------
    // Completion provider — suggest commands at the start of a line
    // -----------------------------------------------------------------------
    const completionProvider = vscode.languages.registerCompletionItemProvider('ppl', {
        provideCompletionItems(document, position) {
            const linePrefix = document.lineAt(position).text.slice(0, position.character).trimStart();
            // Only suggest top-level keywords at the beginning of a line
            if (linePrefix.includes(' ')) return undefined;

            return Object.entries(COMMAND_DOCS)
                .filter(([kw]) => kw in COMMAND_DOCS)
                .map(([kw, doc]) => {
                    const item = new vscode.CompletionItem(kw, vscode.CompletionItemKind.Keyword);
                    item.documentation = new vscode.MarkdownString(doc);
                    return item;
                });
        }
    });
    context.subscriptions.push(completionProvider);


    const statusBar = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 100);
    statusBar.command = 'ppl.runPipeline';
    statusBar.text = '$(play) Run Pipeline';
    statusBar.tooltip = 'Run this .ppl pipeline (ppl command)';
    context.subscriptions.push(statusBar);

    // Show/hide status bar item based on active editor language
    const updateStatusBar = () => {
        const active = vscode.window.activeTextEditor;
        if (active && active.document.languageId === 'ppl') {
            statusBar.show();
        } else {
            statusBar.hide();
        }
    };

    context.subscriptions.push(
        vscode.window.onDidChangeActiveTextEditor(updateStatusBar)
    );
    updateStatusBar();
}

function deactivate() {}

module.exports = { activate, deactivate };
