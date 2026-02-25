const vscode = require('vscode');
const path = require('path');

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

    // Status bar item — shows while a .ppl file is active
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
