import * as vscode from 'vscode';
import { ROOT_NAME } from './extensionIdentity';

export { ROOT_NAME, URI_SCHEME, VIEW_CONTAINER_ID, ELEGANT_NAME } from './extensionIdentity';

export const OUTPUT_FOLDER_NAME = vscode.workspace.getConfiguration(ROOT_NAME).get('compileOutputFolderName', '.output') || '.output';
