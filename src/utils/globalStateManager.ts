import { createHash } from 'crypto';
import * as vscode from 'vscode';
import { Identity, BaseAPI, ProjectPersist } from '../api/base';
import { SocketIOAPI } from '../api/socketio';
import { ExtendedBaseAPI } from '../api/extendedBase';

const keyServerPersists: string = 'overleaf-servers';
const keyPdfViewPersists: string = 'overleaf-pdf-viewers';

export interface ServerPersist {
    name: string;
    url: string;
    login?: {
        userId: string;
        username: string;
        /** Only present while migrating releases that stored credentials in globalState. */
        identity?: Identity;
        projects?: ProjectPersist[]
    };
}
type ServerPersistMap = {[name: string]: ServerPersist};

export interface ProjectSCMPersist {
    enabled: boolean;
    label: string;
    baseUri: string;
    settings: JSON;
}
type ProjectSCMPersistMap = {[name: string]: ProjectSCMPersist};

export interface ProjectSCMPersistMatch {
    serverName: string;
    project: ProjectPersist;
    scm: ProjectSCMPersist;
}

type PdfViewPersist = {
    frequency: number,
    state: any,
};
type PdfViewPersistMap = {[uri: string]: PdfViewPersist};

export class GlobalStateManager {
    private static readonly identityCache = new Map<string, Identity>();

    static async initialize(context: vscode.ExtensionContext): Promise<void> {
        const persists = context.globalState.get<ServerPersistMap>(keyServerPersists, {});
        let migrated = false;
        for (const server of Object.values(persists)) {
            const legacyIdentity = server.login?.identity;
            if (legacyIdentity) {
                await this.storeIdentity(context, server.name, legacyIdentity);
                delete server.login!.identity;
                migrated = true;
                continue;
            }
            if (!server.login) {
                continue;
            }
            const serialized = await context.secrets.get(this.secretKey(server.name));
            if (serialized) {
                const identity = JSON.parse(serialized) as Identity;
                this.assertIdentity(identity, server.name);
                this.identityCache.set(this.cacheKey(context, server.name), identity);
            } else {
                // A login record without its secret cannot authenticate. Treat it
                // as logged out instead of leaving a misleading, half-live state.
                delete server.login;
                migrated = true;
            }
        }
        if (migrated) {
            await context.globalState.update(keyServerPersists, persists);
        }
    }

    static getServers(context:vscode.ExtensionContext): {server:ServerPersist, api:BaseAPI}[] {
        const persists = context.globalState.get<ServerPersistMap>(keyServerPersists, {});
        const servers = Object.values(persists).map(persist => {
            return {
                server: persist,
                api: new BaseAPI(persist.url),
            };
        });

        if (servers.length===0) {
            const url = new URL('https://www.overleaf.com');
            this.addServer(context, url.host, url.href);
            return this.getServers(context);
        } else {
            return servers;
        }
    }

    static addServer(context:vscode.ExtensionContext, name:string, url:string): boolean {
        const persists = context.globalState.get<ServerPersistMap>(keyServerPersists, {});
        if ( persists[name]===undefined ) {
            persists[name] = { name, url };
            context.globalState.update(keyServerPersists, persists);
            return true;
        } else {
            return false;
        }
    }

    static removeServer(context:vscode.ExtensionContext, name:string): boolean {
        const persists = context.globalState.get<ServerPersistMap>(keyServerPersists, {});
        if ( persists[name]!==undefined ) {
            delete persists[name];
            context.globalState.update(keyServerPersists, persists);
            this.identityCache.delete(this.cacheKey(context, name));
            context.secrets.delete(this.secretKey(name)).then(
                undefined,
                (error: unknown) => console.error(error),
            );
            return true;
        } else {
            return false;
        }
    }

    static async loginServer(context:vscode.ExtensionContext, api:BaseAPI, name:string, auth:{[key:string]:string}): Promise<boolean> {
        const persists = context.globalState.get<ServerPersistMap>(keyServerPersists, {});
        const server   = persists[name];

        if (server.login===undefined) {
            const res = auth.cookies ? await api.cookiesLogin(auth.cookies) : await api.passportLogin(auth.email, auth.password);
            if (res.type==='success' && res.identity!==undefined && res.userInfo!==undefined) {
                server.login = {
                    userId: res.userInfo.userId,
                    username: auth.email || res.userInfo.userEmail,
                };
                await this.storeIdentity(context, name, res.identity);
                await context.globalState.update(keyServerPersists, persists);
                return true;
            } else {
                if (res.message!==undefined) {
                    vscode.window.showErrorMessage(res.message);
                }
                return false;
            }
        } else {
            return false;
        }
    }

    static async logoutServer(context:vscode.ExtensionContext, api:BaseAPI, name:string): Promise<boolean> {
        const persists = context.globalState.get<ServerPersistMap>(keyServerPersists, {});
        const server   = persists[name];

        if (server.login!==undefined) {
            const identity = await this.loadIdentity(context, name);
            await api.logout(identity);
            delete server.login;
            this.identityCache.delete(this.cacheKey(context, name));
            await context.secrets.delete(this.secretKey(name));
            await context.globalState.update(keyServerPersists, persists);
            return true;
        } else {
            return false;
        }
    }

    static async fetchServerProjects(context:vscode.ExtensionContext, api:BaseAPI, name:string): Promise<ProjectPersist[]> {
        const persists = context.globalState.get<ServerPersistMap>(keyServerPersists, {});
        const server   = persists[name];

        if (server.login!==undefined) {
            const identity = await this.loadIdentity(context, name);
            let res = await api.getProjectsJson(identity);
            if (res.type!=='success') {
                // fallback to `userProjectsJson`
                res = await api.userProjectsJson(identity);
            }
            if (res.type==='success' && res.projects!==undefined) {
                Object.values(res.projects).forEach(project => {
                    project.userId = (server.login as any).userId;
                });
                const projects = res.projects.map(project => {
                    const existProject = server.login?.projects?.find(p => p.id===project.id);
                    // merge existing scm
                    if (existProject) {
                        project.scm = existProject.scm;
                    }
                    return project;
                });
                server.login.projects = projects;
                context.globalState.update(keyServerPersists, persists);
                return projects;
            } else {
                // regex match for cookie expired
                const cookieExpireRegex = /^302/;
                if (res.message && cookieExpireRegex.test(res.message)) {
                    vscode.window.showErrorMessage(vscode.l10n.t('Cookie Expired. Please Re-Login'));
                    return Promise.reject();
                }
                if (res.message!==undefined) {
                    vscode.window.showErrorMessage(res.message);
                }
                return [];
            }
        } else {
            return [];
        }
    }

    static authenticate(context:vscode.ExtensionContext, name:string) {
        const persists = context.globalState.get<ServerPersistMap>(keyServerPersists, {});
        const server   = persists[name];
        return server.login !== undefined
            ? this.loadIdentity(context, name)
            : Promise.reject(new Error(`Server is not logged in: ${name}`));
    }

    static initSocketIOAPI(context:vscode.ExtensionContext, name:string, projectId:string) {
        const persists = context.globalState.get<ServerPersistMap>(keyServerPersists, {});
        const server   = persists[name];
        const identity = this.identityCache.get(this.cacheKey(context, name));

        if (server.login!==undefined && identity) {
            const api = new ExtendedBaseAPI(server.url);
            const socket = new SocketIOAPI(server.url, api, identity, projectId);
            return {api, socket};
        }
    }

    static getServerProjectSCMPersists(context:vscode.ExtensionContext, serverName:string, projectId:string) {
        const persists = context.globalState.get<ServerPersistMap>(keyServerPersists, {});
        const server   = persists[serverName];
        const project  = server.login?.projects?.find(project => project.id===projectId);
        const scmPersists = project?.scm ? project.scm as ProjectSCMPersistMap : {};
        return scmPersists;
    }

    static findProjectSCMsByBaseUri(
        context: vscode.ExtensionContext,
        label: string,
        baseUri: vscode.Uri,
    ): ProjectSCMPersistMatch[] {
        const persists = context.globalState.get<ServerPersistMap>(keyServerPersists, {});
        const matches: ProjectSCMPersistMatch[] = [];
        for (const server of Object.values(persists)) {
            for (const project of server.login?.projects ?? []) {
                const scms = Object.values((project.scm ?? {}) as ProjectSCMPersistMap);
                for (const scm of scms) {
                    const configured = scm.baseUri.startsWith('/')
                        ? vscode.Uri.file(scm.baseUri)
                        : vscode.Uri.parse(scm.baseUri);
                    if (
                        scm.label === label
                        && (scm.enabled ?? true)
                        && configured.scheme === 'file'
                        && configured.fsPath === baseUri.fsPath
                    ) {
                        matches.push({ serverName: server.name, project, scm });
                    }
                }
            }
        }
        return matches;
    }

    static updateServerProjectSCMPersist(context:vscode.ExtensionContext, serverName:string, projectId:string, scmKey:string, scmPersist?:ProjectSCMPersist) {
        const persists = context.globalState.get<ServerPersistMap>(keyServerPersists, {});
        const server   = persists[serverName];
        const project  = server.login?.projects?.find(project => project.id===projectId);
        if (project) {
            const scmPersists = (project.scm ?? {}) as ProjectSCMPersistMap;
            if (scmPersist===undefined) {
                delete scmPersists[scmKey];
            } else {
                scmPersists[scmKey] = scmPersist;
            }
            project.scm = scmPersists;
            context.globalState.update(keyServerPersists, persists);
        }
    }

    static getPdfViewPersist(context:vscode.ExtensionContext, uri:string): any {
        return context.globalState.get<PdfViewPersistMap>(keyPdfViewPersists, {})[uri]?.state;
    }

    static updatePdfViewPersist(context:vscode.ExtensionContext, uri:string, state:any) {
        const persists = context.globalState.get<PdfViewPersistMap>(keyPdfViewPersists, {});

        // update record
        if (persists[uri]!==undefined) {
            persists[uri].frequency++;
            persists[uri].state = state;
        } else {
            persists[uri] = {frequency: 1, state};
        }

        // when length>=100, remove first least used record
        if (Object.keys(persists).length>=100) {
            let minFrequency = Number.MAX_SAFE_INTEGER;
            let minUri = '';
            Object.entries(persists).forEach(([uri, persist]) => {
                if (persist.frequency<minFrequency) {
                    minFrequency = persist.frequency;
                    minUri = uri;
                }
            });
            delete persists[minUri];
        }

        context.globalState.update(keyPdfViewPersists, persists);
    }

    private static async storeIdentity(
        context: vscode.ExtensionContext,
        name: string,
        identity: Identity,
    ): Promise<void> {
        this.assertIdentity(identity, name);
        await context.secrets.store(this.secretKey(name), JSON.stringify(identity));
        this.identityCache.set(this.cacheKey(context, name), identity);
    }

    private static async loadIdentity(
        context: vscode.ExtensionContext,
        name: string,
    ): Promise<Identity> {
        const cacheKey = this.cacheKey(context, name);
        const cached = this.identityCache.get(cacheKey);
        if (cached) {
            return cached;
        }
        const serialized = await context.secrets.get(this.secretKey(name));
        if (!serialized) {
            throw new Error(`No stored credentials for Overleaf server: ${name}`);
        }
        const identity = JSON.parse(serialized) as Identity;
        this.assertIdentity(identity, name);
        this.identityCache.set(cacheKey, identity);
        return identity;
    }

    private static assertIdentity(identity: Identity, name: string): void {
        if (!identity || typeof identity.cookies !== 'string' || typeof identity.csrfToken !== 'string') {
            throw new Error(`Invalid stored credentials for Overleaf server: ${name}`);
        }
    }

    private static secretKey(name: string): string {
        const digest = createHash('sha256').update(name).digest('hex');
        return `hershel.overleaf-workshop-safe-sync.identity.${digest}`;
    }

    private static cacheKey(context: vscode.ExtensionContext, name: string): string {
        return `${context.extension.id}\0${name}`;
    }

}
