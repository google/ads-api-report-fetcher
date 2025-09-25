#!/usr/bin/env node
/* eslint-disable no-process-exit */
/**
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import * as fs from 'node:fs';
import * as path from 'node:path';
import os from 'node:os';
import child_process from 'child_process';
import minimist from 'minimist';
import inquirer from 'inquirer';
import prompts from 'prompts';
import figlet from 'figlet';
import chalk from 'chalk';
import clui from 'clui';
import yaml from 'js-yaml';
import { generateRefreshToken } from 'google-oauth-authenticator';
const GIT_REPO = 'https://github.com/google/ads-api-report-fetcher.git';
const LOG_FILE = '.create-gaarf-wf-out.log';
const DASHBOARD_LINK_FILE = 'dashboard_url.txt';
const argv = minimist(process.argv.slice(2));
const is_diag = argv.diag;
const is_debug = argv.debug || argv.diag;
const ignore_errors = argv.ignore_errors;
const settings_file = argv.settingsFile || 'settings.ini';
const cwd = getCwd(argv);
function getCwd(argv) {
    // First argument (optional) is a path where the tool run in.
    let cwd = argv._[0];
    if (cwd) {
        if (!path.isAbsolute(cwd)) {
            cwd = path.resolve(process.cwd(), cwd);
        }
        if (!fs.existsSync(cwd)) {
            fs.mkdirSync(cwd);
        }
        process.chdir(cwd);
    }
    else {
        cwd = process.cwd();
    }
    return cwd;
}
function deployShellScript(fileName, content) {
    fs.writeFileSync(fileName, content);
    execSync(`chmod +x ${fileName}`);
    console.log(chalk.gray(`Created ${fileName}`));
}
function execSync(cmd, options) {
    options = Object.assign(options || {}, {
        stdio: 'pipe',
        shell: true,
    });
    const child = child_process.spawnSync(cmd, options);
    if (child.status) {
        return child.stderr.toString();
    }
    return child.stdout.toString();
}
function execCmd(cmd, spinner, options) {
    options = options || {};
    if (spinner && options.realtime === undefined) {
        // having a spinner and streaming stdout at the same looks bad
        options.realtime = false;
    }
    if (is_diag) {
        options.keep_output = true;
        options.realtime = true;
        spinner = null;
    }
    if (spinner)
        spinner.start();
    if (is_debug) {
        console.log(chalk.gray(cmd));
        fs.appendFileSync(LOG_FILE, `[${new Date()}] Running ${cmd}\n`);
    }
    const cp = child_process.spawn(cmd, [], {
        shell: true,
        // inherit stdin, and wrap stdout/stderr
        stdio: ['inherit', 'pipe', 'pipe'],
        cwd: options.cwd,
    });
    return new Promise(resolve => {
        var _a, _b;
        let stderr = '';
        let stdout = '';
        (_a = cp.stderr) === null || _a === void 0 ? void 0 : _a.on('data', chunk => {
            stderr += chunk;
            if (options === null || options === void 0 ? void 0 : options.realtime)
                process.stderr.write(chunk);
        });
        (_b = cp.stdout) === null || _b === void 0 ? void 0 : _b.on('data', chunk => {
            stdout += chunk;
            if (options === null || options === void 0 ? void 0 : options.realtime)
                process.stdout.write(chunk);
        });
        cp.on('close', (code) => {
            if (spinner)
                spinner.stop();
            if (!spinner && (options === null || options === void 0 ? void 0 : options.realtime) && !(options === null || options === void 0 ? void 0 : options.keep_output)) {
                // if there's no spinner and keep_output=false, remove all output of the command
                const terminal_width = process.stdout.columns;
                let lines = (stdout ? stdout.split(os.EOL) : []).concat(stderr ? stderr.split(os.EOL) : []);
                lines = lines.map(line => {
                    const arr = line.split('\r');
                    return arr[arr.length - 1];
                });
                if (lines.length > 0) {
                    const row_count = lines
                        .map(line => ((line.length / terminal_width) | 0) + 1)
                        .reduce((total, count) => (total += count), 0);
                    process.stdout.cursorTo(0);
                    process.stdout.moveCursor(0, -row_count + 1);
                    process.stdout.clearScreenDown();
                }
            }
            if (code !== 0 && !(options === null || options === void 0 ? void 0 : options.realtime) && !(options === null || options === void 0 ? void 0 : options.silent)) {
                // by default, if not switched off and not realtime output, show error
                console.error(stderr || stdout);
            }
            if (is_debug) {
                fs.appendFileSync(LOG_FILE, `[${new Date()}] ${cmd} return ${code} exit code\n`);
                fs.appendFileSync(LOG_FILE, stdout + '\n');
                fs.appendFileSync(LOG_FILE, stderr + '\n');
            }
            resolve({
                code,
                stderr: stderr || '',
                stdout: stdout || '',
            });
        });
    });
}
async function askForGcpRegion(answers) {
    if (answers && answers.gcp_region) {
        return answers.gcp_region;
    }
    // TODO: this is an ugly hard-code but at the moment of Jan 2023 there's no way to automate it
    // The list contains GCP regions where all three services are supported:
    // Cloud Functions (gcloud functions regions list), Cloud Scheduler (gcloud scheduler regions list),
    // Cloud Workflows (no API! only doc - https://cloud.google.com/workflows/docs/locations)
    const regions = [
        'asia-east1',
        'asia-east2',
        'asia-northeast1',
        'asia-northeast2',
        'asia-south1',
        'asia-southeast1',
        'australia-southeast1',
        'europe-central2',
        'europe-west1',
        'europe-west2',
        'europe-west3',
        'europe-west6',
        'northamerica-northeast1',
        'southamerica-east1',
        'us-central1',
        'us-east1',
        'us-east4',
        'us-west1',
        'us-west2',
        'us-west3',
        'us-west4',
    ];
    const MANUAL_ITEM = '__MANUAL__';
    const options = [{ title: 'Enter manually', value: MANUAL_ITEM }].concat(regions.map(item => {
        return {
            title: item,
            value: item,
        };
    }));
    const response = await prompts({
        type: 'autocomplete',
        name: 'gcp_region',
        message: 'Region for Cloud services (workflows, functions, scheduler):',
        choices: options,
    });
    if (response.gcp_region === MANUAL_ITEM || !response.gcp_region) {
        const response2 = await prompts({
            type: 'text',
            name: 'gcp_region',
            validate: val => {
                if (!val) {
                    return 'Please provide a Cloud region';
                }
                return true;
            },
            message: 'Please enter a region for Cloud services:',
        });
        return response2.gcp_region;
    }
    return response.gcp_region;
}
async function initializeGcpProject(answers) {
    // check for gcloud
    const gcloud_res = await execCmd('which gcloud', null, { silent: true });
    const gcloud_path = gcloud_res.stdout.trim();
    if (gcloud_res.code !== 0 || !fs.existsSync(gcloud_path)) {
        console.log(chalk.red('Could not find gcloud command, please make sure you installed Google Cloud SDK') +
            ' - see ' +
            chalk.blue('https://cloud.google.com/sdk/docs/install'));
        process.exit(-1);
    }
    // now check for authentication in gcloud
    const auth_output = execSync('gcloud auth print-access-token')
        .toString()
        .trim();
    if (auth_output.includes('ERROR: (gcloud.auth.print-access-token)')) {
        console.log(chalk.red('Please authenticate in gcloud using ') +
            chalk.white('gcloud auth login'));
        process.exit(-1);
    }
    let gcp_project_id = execSync('gcloud config get-value project 2> /dev/null')
        .toString()
        .trim();
    if (gcp_project_id) {
        if ((await prompt({
            type: 'confirm',
            name: 'use_current_project',
            message: `Detected current GCP project ${chalk.green(gcp_project_id)}, do you want to use it (Y) or choose another (N)?:`,
            default: true,
        }, answers)).use_current_project) {
            return gcp_project_id;
        }
    }
    // otherwise let the user to choose a project
    const projects_csv = execSync('gcloud projects list --format="csv(projectId,name)" --sort-by=projectId --limit=500').toString();
    const rows = projects_csv.split('\n').map(row => row.split(','));
    rows.splice(0, 1); // remove header row
    let options = rows
        .filter((cols) => !!cols[0])
        .map(row => {
        return {
            title: row[0] + (row[1] ? ' (' + row[1] + ')' : ''),
            value: row[0],
        };
    });
    const MANUAL_ITEM = '__MANUAL__';
    options = [{ title: 'Enter manually', value: MANUAL_ITEM }].concat(options);
    let response = await prompts({
        type: 'autocomplete',
        name: 'project_id',
        message: 'Please choose a GCP project:',
        choices: options,
    });
    gcp_project_id = response.project_id;
    if (gcp_project_id === MANUAL_ITEM || !gcp_project_id) {
        response = await prompts({
            type: 'text',
            name: 'project_id',
            validate: val => {
                if (!val) {
                    return 'Please provide a Cloud project id';
                }
                return true;
            },
            message: 'Please enter a GCP project id:',
        });
        // make sure the entered project does exist
        const describe_output = execSync(`gcloud projects describe ${response.project_id}`).toString();
        if (describe_output.includes('ERROR:')) {
            console.log(chalk.red('Could not set current project'));
            console.log(describe_output);
            process.exit(-1);
        }
    }
    gcp_project_id = response.project_id;
    if (gcp_project_id) {
        execSync('gcloud config set project ' + gcp_project_id);
    }
    return gcp_project_id;
}
/**
 * Walks through directory structure and returns a list of full file paths.
 * @param dirpath a directory path
 * @returns a list of full paths of files in the directory (recursively)
 */
function readDir(dirpath) {
    let results = [];
    if (!fs.existsSync(dirpath))
        return results;
    const entries = fs.readdirSync(dirpath, { withFileTypes: true });
    entries.forEach((entry) => {
        const entry_path = path.resolve(dirpath, entry.name);
        if (entry.isDirectory()) {
            /* Recurse into a subdirectory */
            results = results.concat(readDir(entry_path));
        }
        else {
            /* Is a file */
            results.push(entry_path);
        }
    });
    return results;
}
function getMacroValues(folder_path, answers, prefix) {
    const filelist = readDir(folder_path);
    const macro = {};
    for (const file_path of filelist) {
        if (file_path.endsWith('.sql')) {
            let script_content = fs.readFileSync(file_path, 'utf-8');
            // if script_content contains FUNCTIONS block we should cut it off  before searching for macro
            const fn_match = script_content.match(/FUNCTIONS/i);
            if (fn_match === null || fn_match === void 0 ? void 0 : fn_match.index) {
                script_content = script_content.substring(0, fn_match.index);
            }
            // notes on the regexp:
            //  "(?<!\$)" - is a lookbehind expression (catch the following exp if it's
            //  not prepended with '$'), with that we're capturing {smth} expressions
            //  and not ${smth} expressions
            const re = /(?<!\$)\{(?<macro>[^}]+)\}/gi;
            const matches = [...script_content.matchAll(re)];
            for (const match of matches) {
                if (match.groups) {
                    macro[match.groups['macro']] = null;
                }
            }
        }
    }
    const options = Object.keys(macro).map(name => {
        return { type: 'text', name: name, message: name };
    });
    if (options.length) {
        if (!answers[prefix] ||
            options.filter(i => !(i.name in answers[prefix])).length) {
            console.log(`Please enter values for the following macros found in your scripts in '${folder_path}' folder`);
            console.log(chalk.yellow('Tip: ') +
                chalk.gray('you can use constants, :YYYYMMDD-N values (where N is a number) and expressions (${..})') +
                '\n' +
                chalk.yellow('Tip: ') +
                chalk.gray('For macros with dataset names make sure that you meet BigQuery requirements - use letters, numbers and underscores'));
        }
        answers[prefix] = answers[prefix] || {};
        return prompt(options, answers[prefix]);
    }
    return {};
}
async function prompt(questions, answers) {
    const actual_answers = await inquirer.prompt(questions, answers);
    Object.assign(answers, actual_answers);
    return actual_answers;
}
function getLookerstudioCreateReportUrl(report_id, report_name, project_id, dataset_id, datasources) {
    let url = 'https://lookerstudio.google.com/reporting/create?';
    report_name = encodeURIComponent(report_name);
    url += `c.mode=edit&c.reportId=${report_id}&r.reportName=${report_name}&ds.*.refreshFields=false`;
    if (datasources) {
        Object.entries(datasources).map(entries => {
            const alias = entries[0];
            const table = entries[1];
            url +=
                `&ds.${alias}.connector=bigQuery` +
                    `&ds.${alias}.datasourceName=${table}` +
                    `&ds.${alias}.projectId=${project_id}` +
                    `&ds.${alias}.datasetId=${dataset_id}` +
                    `&ds.${alias}.type=TABLE` +
                    `&ds.${alias}.tableId=${table}`;
        });
    }
    return url;
}
async function askForDashboardDatasources(datasources) {
    const idx = Object.keys(datasources).length;
    const questions = [
        {
            type: 'input',
            name: 'dashboard_datasource',
            message: `(${idx}) Enter a datasource alias in Looker Studio dashboard:`,
        },
        {
            type: 'input',
            name: 'dashboard_table',
            message: `(${idx}) Enter a BigQuery table id with data for Looker Studio datasource:`,
            when: answers => !!answers.dashboard_datasource,
        },
        {
            type: 'confirm',
            name: 'dashboard_more_tables',
            message: 'Do you want to enter another datasource:',
            default: false,
            when: answers => !!answers.dashboard_datasource,
        },
    ];
    const answers = await inquirer.prompt(questions);
    if (answers.dashboard_datasource) {
        datasources[answers.dashboard_datasource] = answers.dashboard_table;
    }
    if (answers.dashboard_more_tables) {
        return await askForDashboardDatasources(datasources);
    }
    return datasources;
}
async function deployDashboard(answers, project_id, output_dataset, macro_bq) {
    const dash_answers = await prompt([
        {
            type: 'input',
            name: 'dashboard_id',
            message: 'Looker Studio dashboard id (00000000-0000-0000-0000-000000000000):',
        },
        {
            type: 'input',
            name: 'dashboard_name',
            message: 'Looker Studio dashboard name:',
        },
    ], answers);
    // extract datasource from bq_macros
    const ds_candidates = Object.entries(macro_bq)
        .filter(values => values[0].includes('dataset') &&
        values[1] &&
        values[1] !== output_dataset)
        .map(values => {
        return { title: values[1], value: values[1] };
    })
        .concat({ title: output_dataset, value: output_dataset });
    let dataset_id = answers.dashboard_dataset;
    if (!dataset_id) {
        // TODO: should we provide an ability to enter a dataset manually (could be useful if it's hard-coded in queries)
        if (ds_candidates.length === 1) {
            dataset_id = ds_candidates[0].title;
        }
        else {
            dataset_id = (await prompts({
                type: 'autocomplete',
                name: 'dashboard_dataset',
                message: 'Please choose a BigQuery dataset for report tables:',
                choices: ds_candidates,
            })).dashboard_dataset;
        }
    }
    // for cloning datasources we need BQ table-id AND datasource alias in Looker Studio
    // (see https://developers.google.com/looker-studio/integrate/linking-api#data-source-alias)
    let datasources = answers.dashboard_datasources || {};
    if (Object.keys(datasources).length === 0) {
        datasources = await askForDashboardDatasources(datasources);
        answers.dashboard_datasources = datasources;
    }
    const dashboard_url = getLookerstudioCreateReportUrl(dash_answers.dashboard_id, dash_answers.dashboard_name, project_id, dataset_id, datasources);
    console.log('As soon as your workflow completes successfully, open the following link in the browser for cloning template dashboard (you can find it inside dashboard_url.txt):');
    console.log(chalk.cyanBright(dashboard_url));
    fs.writeFileSync(DASHBOARD_LINK_FILE, dashboard_url);
    return dashboard_url;
}
async function initializeGoogleAdsConfig(answers, serviceAccount, gaarfFolder) {
    const googleads_config_candidate = fs.readdirSync(cwd).find(f => {
        const file_name = path.basename(f);
        return !!(file_name.includes('google-ads') &&
            (file_name.endsWith('yaml') || file_name.endsWith('.yml')));
    });
    const use_googleads_config = (await prompt([
        {
            type: 'confirm',
            name: 'use_googleads_config',
            message: 'Do you want to use an existing google-ads.yaml (Y)?:',
            default: true,
        },
    ], answers)).use_googleads_config;
    let path_to_googleads_config = '';
    if (use_googleads_config) {
        // user wants to use an existing google-ads, we don't care what's inside
        const answers_new = await prompt([
            {
                type: 'input',
                name: 'path_to_googleads_config',
                message: 'Path to your google-ads.yaml:',
                default: googleads_config_candidate || 'google-ads.yaml',
            },
        ], answers);
        path_to_googleads_config = answers_new.path_to_googleads_config;
        if (!fs.existsSync(path_to_googleads_config)) {
            console.log(chalk.yellow('Currently the file ') +
                chalk.cyan(path_to_googleads_config) +
                chalk.yellow(" does not exist, please note that you need to upload it before you can actually deploy and run, after that you'll need to run ") +
                chalk.cyan('deploy-queries.sh'));
        }
        else if (!fs.statSync(path_to_googleads_config).isFile()) {
            console.log(chalk.red('The path to google-ads.yaml you specified does not exist. You can specify a full or relative file path but it should include a file name'));
            process.exit(-1);
        }
        // user has provide a path to google-ads.yaml and it exists, we're done
        return path_to_googleads_config;
    }
    const answers1 = await prompt([
        {
            type: 'list',
            name: 'googleads_credentials_type',
            message: 'How do you want to access Google Ads API',
            default: 'service_account',
            choices: [
                {
                    name: 'Under a Service Account' +
                        (serviceAccount ? ' (' + serviceAccount + ')' : ''),
                    value: 'service_account',
                },
                { name: 'Under a User Account', value: 'user_account' },
            ],
        },
    ], answers);
    const useServiceAccount = answers1.googleads_credentials_type === 'service_account';
    if (useServiceAccount) {
        // For running under a SA we need only dev_token, optionally a MCC and key_file
        const answers2 = await prompt([
            {
                type: 'confirm',
                name: 'use_secret_manager',
                message: 'Do you want to use Secret Manager?:',
                default: true,
            },
        ], answers);
        if (answers2.use_secret_manager) {
            const answers3 = await prompt([
                {
                    type: 'input',
                    name: 'googleads_config_devtoken',
                    message: 'Enter Google Ads API developer token to put into "google-ads-dev-token" secret or leave blank to skip:',
                },
            ], answers);
            if (answers3.googleads_config_devtoken) {
                const res = await execCmd(`./${gaarfFolder}/gcp/setup.sh create_secret --secret google-ads-dev-token --value ${answers3.googleads_config_devtoken}`);
                if (res.code !== 0 && !ignore_errors) {
                    process.exit(res.code);
                }
            }
            else {
                console.log(chalk.yellow('You need to create a secret ') +
                    chalk.cyan('google-ads-dev-token') +
                    chalk.yellow(' with a dev token before calling the workflow. '));
                console.log('To do this run the command: ' +
                    chalk.cyan(`./${gaarfFolder}/gcp/setup.sh create_secret --secret google-ads-dev-token --value <YOUR_DEV_TOKEN>`));
            }
            // TODO: what about MCC?
            // regardless of whether the secret was created we won't use google-ads.yaml, we're done
            return null;
        }
        // otherwise, we use service account but with google-ads.yaml
    }
    // prompting the user for credentials to put into google-ads.yaml;
    // either for a User Account (need refresh_token, client_id, client_secret, dev_token)
    // or Service Account (need dev_token)
    let refresh_token = '';
    const answers_new = await prompt([
        {
            type: 'input',
            name: 'googleads_config_clientid',
            message: 'OAuth client id:',
            when: () => !useServiceAccount,
        },
        {
            type: 'input',
            name: 'googleads_config_clientsecret',
            message: 'OAuth client secret:',
            when: () => !useServiceAccount,
        },
        {
            type: 'input',
            name: 'googleads_config_devtoken',
            message: 'Google Ads API developer token:',
        },
        {
            type: 'input',
            name: 'googleads_config_mcc',
            message: 'Google Ads MCC:',
        },
        {
            type: 'confirm',
            name: 'googleads_config_generate_refreshtoken',
            when: () => !useServiceAccount,
            // TODO: add a note that it won't work in Cloud Shell
            message: 'Do you want to generate a refresh token (Y) or you will enter it manually (N)?:',
        },
    ], answers);
    if (answers_new.googleads_config_generate_refreshtoken) {
        const flow = await generateRefreshToken(answers_new.googleads_config_clientid, answers_new.googleads_config_clientsecret, 'https://www.googleapis.com/auth/adwords');
        console.log('Navigate to the following url on the current machine:');
        console.log(chalk.cyan(flow.authorizeUrl));
        refresh_token = await flow.getToken();
        if (refresh_token) {
            console.log('Successfully acquired a refresh token');
        }
    }
    else {
        refresh_token = (await prompt([
            {
                type: 'input',
                name: 'googleads_config_refreshtoken',
                message: 'Enter refresh token:',
                when: () => !useServiceAccount,
            },
        ], answers)).googleads_config_refreshtoken;
    }
    // google-ads.yaml wasn't specified (credentials were entered),
    // so let's create it under the default name
    path_to_googleads_config = 'google-ads.yaml';
    const yaml_content = `# File was generated with create-gaarf-wf at ${new Date()}
developer_token: ${answers_new.googleads_config_devtoken || ''}
login_customer_id: ${sanitizeCustomerId(answers_new.googleads_config_mcc || '')}` +
        (useServiceAccount
            ? ''
            : `
client_id: ${answers_new.googleads_config_clientid || ''}
client_secret: ${answers_new.googleads_config_clientsecret || ''}
refresh_token: ${refresh_token || ''}
    `);
    fs.writeFileSync(path_to_googleads_config, yaml_content, {
        encoding: 'utf8',
    });
    console.log(`Google Ads API credentials were saved to ${path_to_googleads_config}`);
    return path_to_googleads_config;
}
async function validateGoogleAdsConfig(gaarf_folder, path_to_googleads_config) {
    await execCmd('npm install --prod', new clui.Spinner('Installing dependencies...'), {
        cwd: `./${gaarf_folder}/js`,
    });
    const res = await execCmd(`./gaarf validate --ads-config=../../${path_to_googleads_config} --api=rest`, new clui.Spinner(`Validating Ads credentials from ${path_to_googleads_config}...`), { cwd: `./${gaarf_folder}/js` });
    if (res.code !== 0 && !ignore_errors) {
        process.exit(res.code);
    }
}
function getAnswers() {
    let answers = {};
    if (argv.answers) {
        // users can mistakenly supply `--answers.json` (instead of `answers=answers.json`), in that case argv.answers
        if (typeof argv.answers !== 'string') {
            console.log(chalk.red('Argument answers does not seem to have a correct value (a file name): ' +
                JSON.stringify(argv.answers)));
            process.exit(-1);
        }
        let answersContent;
        try {
            answersContent = fs.readFileSync(argv.answers, 'utf-8');
        }
        catch (e) {
            console.log(chalk.red(`Answers file ${argv.answers} could not be found or read.`));
            process.exit(-1);
        }
        answers = JSON.parse(answersContent) || {};
        console.log(`Using answers from '${argv.answers}' file`);
    }
    return answers;
}
function getMultiRegion(region) {
    if (!region)
        return 'eu'; // europe by default
    if (region.includes('us'))
        return 'us';
    if (region.includes('europe'))
        return 'eu';
    if (region.includes('asia'))
        return 'asia';
    return region;
}
function sanitizeCustomerId(cid) {
    return cid.toString().replaceAll('-', '').replaceAll(' ', '');
}
async function gitCloneRepo(url, gaarf_folder) {
    if (!fs.existsSync(gaarf_folder)) {
        await execCmd(`git clone ${url} --depth 1 ${gaarf_folder}`, new clui.Spinner(`Cloning Gaarf repository (${url}), please wait...`));
    }
    else {
        let git_user_name = '';
        try {
            git_user_name = execSync('git config --get user.name', {
                cwd: path.join(cwd, gaarf_folder),
            })
                .toString()
                .trim();
            // eslint-disable-next-line no-empty
        }
        catch (e) {
            // no user identity in git, that's ok
        }
        if (!git_user_name) {
            // there's no user identity, git pull -ff can fail, let's set some arbitrary identity
            const git_user_name = execSync('echo $USER').toString().trim() || 'user';
            const git_user_email = execSync('echo $USER_EMAIL').toString().trim() || 'user@example.com';
            execSync(`git config --local user.name ${git_user_name}`, {
                cwd: path.join(cwd, gaarf_folder),
            });
            execSync(`git config --local user.email ${git_user_email}`, {
                cwd: path.join(cwd, gaarf_folder),
            });
        }
        execSync('git pull --ff', { cwd: path.join(cwd, gaarf_folder) });
    }
}
function dumpSettings(settings) {
    let content = '';
    for (const name of Object.keys(settings)) {
        content += `[${name}]\n`;
        const section = settings[name];
        for (const key of Object.keys(section)) {
            if (section[key]) {
                content += `  ${key}=${section[key]}\n`;
            }
        }
    }
    return content;
}
async function init() {
    const answers = getAnswers();
    const status_log = `Running create-gaarf-wf in ${cwd}`;
    if (is_debug) {
        fs.writeFileSync(LOG_FILE, `[${new Date()}]${status_log}`);
    }
    console.log(chalk.gray(status_log));
    console.log(chalk.yellow(figlet.textSync('Gaarf Workflow', { horizontalLayout: 'full' })));
    console.log(`Welcome to interactive generator for Gaarf Workflow (${chalk.redBright('G')}oogle ${chalk.redBright('A')}ds ${chalk.redBright('A')}PI ${chalk.redBright('R')}eport ${chalk.redBright('F')}etcher Workflow)`);
    console.log('You will be asked a bunch of questions to prepare and initialize your Cloud infrastructure');
    console.log('It is best to run this script in a folder that is a parent for your queries');
    // clone gaarf repo
    const gaarf_folder = 'ads-api-fetcher';
    await gitCloneRepo(GIT_REPO, gaarf_folder);
    const gcp_project_id = await initializeGcpProject(answers);
    const PATH_ADS_QUERIES = 'ads-queries';
    const PATH_BQ_QUERIES = 'bq-queries';
    const name = (await prompt([
        {
            type: 'input',
            name: 'name',
            message: 'Your project name (spaces/underscores will be converted to "-"):',
            default: path.basename(cwd),
            filter: value => {
                return value.replaceAll(' ', '-').replaceAll('_', '-');
            },
        },
    ], answers)).name;
    answers.name = name;
    const ads_queries_folder_candidates = fs
        .readdirSync(cwd)
        .find(f => path.basename(f).includes('ads') &&
        path.basename(f).includes('queries'));
    const bq_queries_folder_candidates = fs
        .readdirSync(cwd)
        .find(f => path.basename(f).includes('bq') && path.basename(f).includes('queries'));
    const answers1 = await prompt([
        {
            type: 'input',
            name: 'path_to_ads_queries',
            message: 'Relative path to a folder with your Ads queries:',
            default: ads_queries_folder_candidates || PATH_ADS_QUERIES,
        },
        {
            type: 'input',
            name: 'path_to_bq_queries',
            message: 'Relative path to a folder with your BigQuery queries:',
            default: bq_queries_folder_candidates || PATH_BQ_QUERIES,
        },
        {
            type: 'input',
            name: 'gcs_bucket',
            message: 'GCP bucket name for queries:',
            default: gcp_project_id,
            filter: value => {
                return value.startsWith('gs://')
                    ? value.substring('gs://'.length)
                    : value;
            },
        },
        {
            type: 'input',
            name: 'service_account',
            message: 'Custom service account name (leave blank to use the default one):',
        },
        {
            type: 'input',
            name: 'custom_ids_query_path',
            message: 'File path with a query to filter customer accounts (leave blank if not needed):',
            default: '',
        },
    ], answers);
    const path_to_ads_queries = answers1.path_to_ads_queries;
    const path_to_ads_queries_abs = path.join(cwd, path_to_ads_queries);
    const path_to_bq_queries = answers1.path_to_bq_queries;
    const path_to_bq_queries_abs = path.join(cwd, path_to_bq_queries);
    let gcs_bucket = answers1.gcs_bucket;
    const service_account = answers1.service_account;
    if (!fs.existsSync(path_to_ads_queries_abs)) {
        fs.mkdirSync(path_to_ads_queries_abs);
        console.log(chalk.grey(`Created '${path_to_ads_queries_abs}' folder`));
    }
    if (!fs.existsSync(path_to_bq_queries_abs)) {
        fs.mkdirSync(path_to_bq_queries_abs);
        console.log(chalk.grey(`Created '${path_to_bq_queries_abs}' folder`));
    }
    const custom_ids_query_path = answers1.custom_ids_query_path;
    const path_to_googleads_config = await initializeGoogleAdsConfig(answers, service_account, gaarf_folder);
    // some warnings to users if queries and ads config don't exist
    if (!fs.readdirSync(path_to_ads_queries).length) {
        console.log(chalk.red(`Please place your ads scripts into '${path_to_ads_queries}' folder`));
    }
    gcs_bucket = (gcs_bucket || gcp_project_id).trim();
    // call the gaarf cli tool from the cloned repo to validate the ads credentials
    if (path_to_googleads_config &&
        fs.existsSync(path_to_googleads_config) &&
        !answers.disable_ads_validation) {
        await validateGoogleAdsConfig(gaarf_folder, path_to_googleads_config);
    }
    const gcp_region = await askForGcpRegion(answers);
    // create a bucket if it doesn't exist
    // TODO: move this to setup.sh and just call it with create_bucket task
    let res = await execCmd(`gsutil ls gs://${gcs_bucket}`, new clui.Spinner(`Checking if GCS bucket ${gcs_bucket} exists`), { silent: true });
    if (res.code !== 0) {
        // bucket doesn't exist
        res = await execCmd(`gsutil mb -l ${getMultiRegion(gcp_region)} -b on gs://${gcs_bucket}`, new clui.Spinner(`Creating a GCS bucket ${gcs_bucket}`), { silent: true });
        if (res.code !== 0) {
            console.log(chalk.red(`Could not create a bucket ${gcs_bucket}. Most likely the installation will fail`));
            console.log(res.stderr || res.stdout);
        }
    }
    const gcs_base_path = `gs://${gcs_bucket}/${name}`;
    // Create deploy-queries.sh
    let deploy_custom_query_snippet = '';
    let custom_query_gcs_path;
    if (custom_ids_query_path) {
        if (!fs.existsSync(custom_ids_query_path)) {
            console.log(chalk.red(`Could not find script '${custom_ids_query_path}'`));
        }
        custom_query_gcs_path = `${gcs_base_path}/get-accounts.sql`;
        deploy_custom_query_snippet = `gsutil -m cp ${custom_ids_query_path} $GCS_BASE_PATH/get-accounts.sql`;
    }
    let deploy_googleads_config_snippet = '';
    if (path_to_googleads_config) {
        deploy_googleads_config_snippet = `if [[  -f ${path_to_googleads_config} ]]; then
  gsutil -m cp ${path_to_googleads_config} $GCS_BASE_PATH/google-ads.yaml
fi`;
    }
    // Note that we deploy queries to hard-coded paths
    deployShellScript('deploy-queries.sh', `# Deploy Ads and BQ queries from local folders to Google Cloud Storage.
GCS_BASE_PATH=${gcs_base_path}

${deploy_googleads_config_snippet}
${deploy_custom_query_snippet}

gsutil -m rm -r $GCS_BASE_PATH/${path_to_ads_queries}
if ls ./${path_to_ads_queries}/* 1> /dev/null 2>&1; then
  gsutil -m cp -R ./${path_to_ads_queries}/* $GCS_BASE_PATH/${PATH_ADS_QUERIES}/
fi

gsutil -m rm -r $GCS_BASE_PATH/${path_to_bq_queries}
if ls ./${path_to_bq_queries}/* 1> /dev/null 2>&1; then
  gsutil -m cp -R ./${path_to_bq_queries}/* $GCS_BASE_PATH/${PATH_BQ_QUERIES}/
fi
`);
    // Create deploy-wf.sh
    const cf_memory = (await prompt([
        {
            type: 'list',
            message: 'Memory limit for the Cloud Functions:',
            name: 'cf_memory',
            default: '512MB',
            choices: [
                '128MB',
                '256MB',
                '512MB',
                '1024MB',
                '2048MB',
                '4096MB',
                '8192MB',
            ],
        },
    ], answers)).cf_memory;
    // create settings.ini:
    const settings = {
        common: {
            name: name,
            region: gcp_region,
            'service-account': service_account,
        },
        functions: {
            memory: cf_memory,
        },
    };
    if (!path_to_googleads_config) {
        // not using google-ads.yaml means using Secret Manager
        settings['functions']['use-secret-manager'] = true;
    }
    let aux_args = '';
    if (answers.disable_grants) {
        aux_args = '--disable-grants';
    }
    deployShellScript('deploy-wf.sh', `# Deploy Cloud Functions and Cloud Workflows
set -e
cd ./${gaarf_folder}
git pull --ff
cd ..
./${gaarf_folder}/gcp/setup.sh deploy_all --settings $(readlink -f "./${settings_file}") ${aux_args}
`);
    // now we need parameters for running the WF
    let ads_customer_id;
    if (path_to_googleads_config && fs.existsSync(path_to_googleads_config)) {
        const yamldoc = yaml.load(fs.readFileSync(path_to_googleads_config, 'utf-8'));
        // look up for the default default value for account id (CID) from google-ads.yaml
        ads_customer_id =
            yamldoc['customer_id'] ||
                yamldoc['client_customer_id'] ||
                yamldoc['login_customer_id'];
    }
    const answers2 = await prompt([
        {
            type: 'input',
            name: 'customer_id',
            message: 'Ads account id (customer id, or a list of ids via ","):',
            default: ads_customer_id,
        },
        {
            type: 'input',
            name: 'output_dataset',
            message: 'BigQuery dataset for ads queries results ("-" will be converted to "_"):',
            default: name + '_ads',
            filter(input) {
                return input.replace(/[ -]/g, '_');
            },
        },
    ], answers);
    // now we detect macro used in queries and ask for their values
    const macro_ads = await getMacroValues(path.join(cwd, path_to_ads_queries), answers, 'ads_macro');
    const macro_bq = await getMacroValues(path.join(cwd, path_to_bq_queries), answers, 'bq_macro');
    const writer_options = answers.writer_options;
    const bq_location = gcp_region && gcp_region.startsWith('europe') ? 'europe' : '';
    const output_dataset = answers2.output_dataset;
    const customer_id = sanitizeCustomerId(answers2.customer_id);
    const wf_data = {
        cloud_function: name,
        gcs_bucket: gcs_bucket,
        location: gcp_region,
        ads_queries_path: `${name}/${PATH_ADS_QUERIES}/`,
        bq_queries_path: `${name}/${PATH_BQ_QUERIES}/`,
        dataset: output_dataset,
        cid: customer_id,
        ads_config_path: '',
        output_path: `${gcs_base_path}/tmp`,
        customer_ids_query: custom_query_gcs_path,
        bq_dataset_location: bq_location,
        writer_options: writer_options,
        ads_macro: macro_ads,
        bq_macro: macro_bq,
    };
    if (path_to_googleads_config) {
        wf_data['ads_config_path'] = `${gcs_base_path}/google-ads.yaml`;
    }
    const wf_data_file = 'data.json';
    fs.writeFileSync(wf_data_file, JSON.stringify(wf_data, null, 2));
    // Create run-wf.sh
    deployShellScript('run-wf.sh', `set -e
# run workflow synchronously with parameters in data.json
./ads-api-fetcher/gcp/setup.sh run_wf --settings $(readlink -f "./${settings_file}") --data $(readlink -f "./${wf_data_file}")

# alternatively rub Schedule Job created earlier via schedule-wf.sh
#./ads-api-fetcher/gcp/setup.sh run_job --settings $(readlink -f "./${settings_file}")
`);
    fs.writeFileSync(settings_file, dumpSettings(settings));
    // now execute some scripts
    // deploying queries and ads config to GCS
    if ((await prompt({
        type: 'confirm',
        name: 'deploy_scripts',
        message: 'Do you want to deploy queries (Ads/BQ) to GCS (deploy-queries.sh):',
        default: true,
    }, answers)).deploy_scripts) {
        const res = await execCmd(path.join(cwd, './deploy-queries.sh'), null, {
            realtime: true,
        });
        if (res.code !== 0 && !ignore_errors) {
            console.log(chalk.red('Queries deployment (deploy-queries.sh) failed, breaking'));
            process.exit(res.code);
        }
    }
    else {
        console.log(chalk.yellow("Please note that before you deploy queries to GCS (deploy-queries.sh) you can't run the workflow (it'll fail)"));
    }
    if ((await prompt({
        type: 'confirm',
        name: 'deploy_wf',
        message: 'Do you want to deploy Cloud components (deploy-wf.sh):',
        default: true,
    }, answers)).deploy_wf) {
        // deploying GCP components
        const res = await execCmd(path.join(cwd, './deploy-wf.sh'), new clui.Spinner('Deploying Cloud components, please wait...'));
        if (res.code !== 0 && !ignore_errors) {
            console.log(chalk.red('Cloud components deployment (deploy-wf.sh) failed, breaking'));
            process.exit(res.code);
        }
    }
    else {
        console.log(chalk.yellow("Please note that before you deploy cloud components (deploy-wf.sh) there's no sense in running a scheduler job"));
    }
    const answers3 = await prompt({
        type: 'confirm',
        name: 'schedule_wf',
        message: 'Do you want to schedule a job for executing workflow:',
        default: true,
    }, answers);
    let schedule_cron = '0 0 * * *';
    if (answers3.schedule_wf) {
        const answers_schedule = await prompt([
            {
                type: 'input',
                name: 'schedule_time',
                message: 'Enter time (hh:mm) for job to start:',
                default: '00:00',
                validate: (input) => !input.match(/\d+(:\d+)*/gi) ? 'Please use the format 00:00' : true,
            },
            {
                type: 'confirm',
                name: 'run_job',
                default: false,
                message: "Do you want to run the job right now (it's asynchronous):",
            },
        ], answers);
        const time_parts = answers_schedule.schedule_time.split(':');
        schedule_cron = `${time_parts.length > 1 ? time_parts[1] : 0} ${time_parts[0]} * * *`;
        answers3.run_job = answers_schedule.run_job;
    }
    settings['scheduler'] = settings['scheduler'] || {};
    settings['scheduler']['schedule'] = schedule_cron;
    fs.writeFileSync(settings_file, dumpSettings(settings));
    // Create schedule-wf.sh
    deployShellScript('schedule-wf.sh', `# Create Scheduler Job to execute Cloud Workflow
./${gaarf_folder}/gcp/setup.sh schedule_wf --settings $(readlink -f "./${settings_file}") --data $(readlink -f "./${wf_data_file}")
`);
    if (answers3.schedule_wf) {
        const res = await execCmd(path.join(cwd, './schedule-wf.sh'), new clui.Spinner('Creating a Scheduler Job, please wait...'));
        if (res.code === 0) {
            console.log('Created a Scheduler Job. You can recreate it with different settings by running schedule-wf.sh');
        }
        if (answers3.run_job) {
            // run the job
            const res = await execCmd(`./${gaarf_folder}/gcp/setup.sh schedule_wf --settings $(readlink -f "./${settings_file}")`, null, { realtime: true });
            if (res.code !== 0 && !ignore_errors) {
                console.log(chalk.red('Starting the Scheduler Job has failed, breaking'));
                process.exit(res.code);
            }
        }
    }
    if (!answers3.run_job) {
        // Scheduler Job wasn't run, maybe the user want to run the workflow directly (it's synchronous in contrast to the scheduler)
        const answers_wf = await prompt([
            {
                type: 'confirm',
                name: 'run_wf',
                message: "Do you want to run the workflow right now (it's synchronous):",
            },
        ], answers);
        if (answers_wf.run_wf) {
            const res = await execCmd(path.join(cwd, './run-wf.sh'), null, {
                realtime: true,
            });
            if (res.code !== 0 && !ignore_errors) {
                console.log(chalk.red('Running workflow (run-wf.sh) has failed, breaking'));
                process.exit(res.code);
            }
        }
    }
    // creating scripts for directly executing gaarf
    const adsMacroCliStr = Object.entries(macro_ads)
        .map(macro => `--macro.${macro[0]}=${macro[1]}`)
        .join(' ');
    deployShellScript('run-gaarf-console.sh', `${gaarf_folder}/js/gaarf ${path_to_ads_queries}/*.sql --account=${customer_id} ${path_to_googleads_config ? '--ads-config=' + path_to_googleads_config : ''} --output=console --console.transpose=always ${adsMacroCliStr} --api=rest`);
    deployShellScript('run-gaarf.sh', `${gaarf_folder}/js/gaarf ${path_to_ads_queries}/*.sql --account=${customer_id} ${path_to_googleads_config ? '--ads-config=' + path_to_googleads_config : ''} --output=bq --bq.project=${gcp_project_id} --bq.dataset=${output_dataset} ${adsMacroCliStr} --api=rest`);
    const bqMacroCliStr = Object.entries(macro_bq)
        .map(macro => `--macro.${macro[0]}=${macro[1]}`)
        .join(' ');
    deployShellScript('run-gaarf-bq.sh', `${gaarf_folder}/js/gaarf-bq ${path_to_bq_queries}/*.sql --project=${gcp_project_id} ${bqMacroCliStr}`);
    // clone dashboard
    if ((await prompt({
        type: 'confirm',
        name: 'clone_dashboard',
        message: 'Do you want to clone a Looker Studio dashboard:',
        default: false,
    }, answers)).clone_dashboard) {
        await deployDashboard(answers, gcp_project_id, output_dataset, macro_bq);
        await execCmd(`gsutil cp ${DASHBOARD_LINK_FILE} ${gcs_base_path}/`, new clui.Spinner(`Copying ${DASHBOARD_LINK_FILE} to GCS ${gcs_base_path}/`), { silent: true });
    }
    // at last stage we'll copy all shell scripts to same GCS bucket in scrips folders, so another users could manage the project easily
    await execCmd(`gsutil -m cp *.sh ${gcs_base_path}/scripts/;gsutil -m cp ${settings_file} ${gcs_base_path}/scripts/;gsutil -m cp ${wf_data_file} ${gcs_base_path}/scripts/`, new clui.Spinner(`Copying all shell scripts to GCS ${gcs_base_path}/scripts`), { silent: true });
    // create download-script.sh shell script to download scripts back from GCS
    deployShellScript('download-scripts.sh', `for file in *.sh; do
  [ -e "$file" ] || continue
  cp -- "$file" "$\{file}.bak"
done
gsutil -m cp ${gcs_base_path}/scripts/*.sh .
gsutil -m cp ${gcs_base_path}/scripts/${settings_file} .
gsutil -m cp ${gcs_base_path}/scripts/${wf_data_file} .
`);
    console.log(`All generated shell scripts were uploaded to GCS ${chalk.cyan(gcs_base_path + '/scripts')}`);
    console.log(chalk.green('All done'));
    console.log(chalk.yellow('Tips for using the generated scripts:'));
    console.log(` 🔹 ${chalk.cyan('deploy-queries.sh')} - redeploy queries and google-ads.yaml to GCS`);
    console.log(` 🔹 ${chalk.cyan('deploy-wf.sh')} - redeploy Cloud Functions and Workflow`);
    console.log(` 🔹 ${chalk.cyan('run-wf.sh')} - execute workflow directly, see arguments in ${wf_data_file}`);
    console.log(` 🔹 ${chalk.cyan('schedule-wf.sh')} - reschedule workflow execution, see arguments in ${wf_data_file}`);
    console.log(` 🔹 ${chalk.cyan('run-gaarf-*.sh')} - scripts for direct query execution via gaarf (via command line)`);
    const saveAnswers = argv.save || argv.saveAnswers;
    if (saveAnswers) {
        const output_file = saveAnswers === true ? 'answers.json' : saveAnswers;
        fs.writeFileSync(output_file, JSON.stringify(answers, null, 2));
        console.log(chalk.gray(`Answers saved into ${output_file}`));
    }
}
init().catch(e => {
    console.error(e);
});
//# sourceMappingURL=index.js.map