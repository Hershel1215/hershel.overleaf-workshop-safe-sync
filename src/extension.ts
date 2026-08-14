import * as vscode from 'vscode';
import { ROOT_NAME, URI_SCHEME } from './consts';

import { RemoteFileSystemProvider, VirtualFileSystem } from './core/remoteFileSystemProvider';
import { ProjectManagerProvider } from './core/projectManagerProvider';
import { PdfViewEditorProvider } from './core/pdfViewEditorProvider';
import { CompileManager } from './compile/compileManager';
import { LangIntellisenseProvider } from './intellisense';
import { LocalReplicaSCMProvider } from './scm/localReplicaSCM';
import { GlobalStateManager } from './utils/globalStateManager';
import { registerSafeSyncE2E } from './sync/e2eRunner';
import { createLocalReplicaBinding, ensureLocalReplicaBinding } from './sync/localBinding';

export async function activate(context: vscode.ExtensionContext) {
    await GlobalStateManager.initialize(context);
    context.subscriptions.push(
        vscode.commands.registerCommand(
            `${ROOT_NAME}.safeSync.showReport`,
            () => LocalReplicaSCMProvider.showActiveReport(),
        ),
        vscode.commands.registerCommand(
            `${ROOT_NAME}.safeSync.copyAiGatewayCommand`,
            () => LocalReplicaSCMProvider.copyActiveAiGatewayCommand(),
        ),
    );
    // Register: [core] RemoteFileSystemProvider
    const remoteFileSystemProvider = new RemoteFileSystemProvider(context);
    context.subscriptions.push( ...remoteFileSystemProvider.triggers );

    // Register: [core] ProjectManagerProvider on Activitybar
    const projectManagerProvider = new ProjectManagerProvider(context);
    context.subscriptions.push( ...projectManagerProvider.triggers );

    // Register: [core] PdfViewEditorProvider
    const pdfViewEditorProvider = new PdfViewEditorProvider(context);
    context.subscriptions.push( ...pdfViewEditorProvider.triggers );

    // Register: [compile] CompileManager on Statusbar
    const compileManager = new CompileManager(remoteFileSystemProvider);
    context.subscriptions.push( ...compileManager.triggers );

    // Register: [intellisense] LangIntellisenseProvider
    const langIntellisenseProvider = new LangIntellisenseProvider(context, remoteFileSystemProvider);
    context.subscriptions.push( ...langIntellisenseProvider.triggers );

    // Recover the bound server and start its replica owner before the E2E
    // startup hook can observe configuration or filesystem semantics.
    try {
        let setting = await LocalReplicaSCMProvider.readSettings();
        const workspaceRoot = vscode.workspace.workspaceFolders?.length === 1
            ? vscode.workspace.workspaceFolders[0].uri
            : undefined;
        if (!setting && workspaceRoot?.scheme === 'file') {
            const matches = GlobalStateManager.findProjectSCMsByBaseUri(
                context,
                LocalReplicaSCMProvider.label,
                workspaceRoot,
            );
            if (matches.length > 1) {
                throw new Error('Multiple Overleaf projects claim this local replica directory');
            }
            const match = matches[0];
            if (match) {
                const remoteUri = vscode.Uri.parse(
                    `${URI_SCHEME}://${match.serverName}/${encodeURIComponent(match.project.name)}`
                    + `?user=${match.project.userId}&project=${match.project.id}`,
                );
                await ensureLocalReplicaBinding(
                    workspaceRoot.fsPath,
                    createLocalReplicaBinding(
                        remoteUri.toString(),
                        match.serverName,
                        match.project.name,
                        match.serverUrl,
                    ),
                    match.project.id,
                );
                setting = await LocalReplicaSCMProvider.readSettings();
            }
        }
        if (setting?.uri) {
            const uri = vscode.Uri.parse(setting.uri);
            if (uri.scheme===URI_SCHEME) {
                // A new VS Code extension host can see the workspace binding
                // before its non-secret globalState snapshot. Rehydrate that
                // public record from the binding plus shared SecretStorage.
                await GlobalStateManager.restoreBoundServer(context, setting);
                // activate vfs
                const vfs = (await (await vscode.commands.executeCommand(`${ROOT_NAME}.remoteFileSystem.prefetch`, uri))) as VirtualFileSystem;
                await vfs.init();
                vscode.commands.executeCommand('setContext', `${ROOT_NAME}.activate`, true);
                // activate compile & preview
                if (setting?.enableCompileNPreview) {
                    vscode.commands.executeCommand('setContext', `${ROOT_NAME}.activateCompile`, true);
                }
            }
        }
    } catch (error) {
        console.warn('Safe-sync local binding activation failed', error);
        const reason = error instanceof Error ? error.message : String(error);
        vscode.window.showErrorMessage(
            `Hershel safe sync did not start for this workspace: ${reason}. `
            + 'Edits are not managed until this error is resolved.',
        );
    }
    context.subscriptions.push(...registerSafeSyncE2E(context));
}

export function deactivate() {
    vscode.commands.executeCommand('setContext', `${ROOT_NAME}.activate`, false);
    vscode.commands.executeCommand('setContext', `${ROOT_NAME}.activateCompile`, false);
}
