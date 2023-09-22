#!/usr/bin/env node
/* eslint-disable no-process-exit */
/**
 * Copyright 2022 Google LLC
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
const cwd = get_cwd(argv);
function get_cwd(argv) {
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
function deploy_shell_script(fileName, content) {
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
function exec_cmd(cmd, spinner, options) {
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
            if (stderr && !(options === null || options === void 0 ? void 0 : options.realtime) && !(options === null || options === void 0 ? void 0 : options.silent) && code !== 0) {
                // by default, if not switched off and not realtime output, show error
                console.log(stderr);
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
async function get_gcp_regions(answers) {
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
    //options = [{title: 'Enter manually', value: MANUAL_ITEM}].concat(options);
    const response = await prompts({
        type: 'autocomplete',
        name: 'gcp_region',
        message: 'Region for Cloud services (workflows, functions, scheduler):',
        choices: options,
    });
    return response.gcp_region;
}
async function initialize_gcp_project(answers) {
    // check for gcloud
    const gcloud_res = await exec_cmd('which gcloud', null, { silent: true });
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
            message: `Detected currect GCP project ${chalk.green(gcp_project_id)}, do you want to use it (Y) or choose another (N)?:`,
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
 * Walks throught directory structure and returns a list of full file paths.
 * @param dirpath a directory path
 * @returns a list of full paths of files in the directory (recursively)
 */
function read_dir(dirpath) {
    let results = [];
    const entries = fs.readdirSync(dirpath, { withFileTypes: true });
    entries.forEach((entry) => {
        const entry_path = path.resolve(dirpath, entry.name);
        if (entry.isDirectory()) {
            /* Recurse into a subdirectory */
            results = results.concat(read_dir(entry_path));
        }
        else {
            /* Is a file */
            results.push(entry_path);
        }
    });
    return results;
}
function get_macro_values(folder_path, answers, prefix) {
    const filelist = read_dir(folder_path);
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
            //  not precended with '$'), with that we're capturing {smth} expressions
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
                chalk.gray('besides constants you can use :YYYYMMDD-N values and expressions (${..})') +
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
function get_lookerstudio_create_report_url(report_id, report_name, project_id, dataset_id, datasources) {
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
async function ask_for_dashboard_datasources(datasources) {
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
        return await ask_for_dashboard_datasources(datasources);
    }
    return datasources;
}
async function deploy_dashboard(answers, project_id, output_dataset, macro_bq) {
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
        datasources = await ask_for_dashboard_datasources(datasources);
        answers.dashboard_datasources = datasources;
    }
    const dashboard_url = get_lookerstudio_create_report_url(dash_answers.dashboard_id, dash_answers.dashboard_name, project_id, dataset_id, datasources);
    console.log('As soon as your workflow completes successfully, open the following link in the browser for cloning template dashboard (you can find it inside dashboard_url.txt):');
    console.log(chalk.cyanBright(dashboard_url));
    fs.writeFileSync(DASHBOARD_LINK_FILE, dashboard_url);
    return dashboard_url;
}
async function initialize_googleads_config(answers) {
    const googleads_config_candidate = fs.readdirSync(cwd).find(f => {
        const file_name = path.basename(f);
        return !!(file_name.includes('google-ads') &&
            (file_name.endsWith('yaml') || file_name.endsWith('.yml')));
    });
    const answers_new = await prompt([
        {
            type: 'confirm',
            name: 'use_googleads_config',
            message: 'Do you want to use a google-ads.yaml config (Y) or enter credentials one by one (N)?:',
            default: true,
        },
    ], answers);
    let path_to_googleads_config = '';
    if (answers_new.use_googleads_config) {
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
                chalk.cyan('deploy-scripts.sh'));
        }
        else if (!fs.statSync(path_to_googleads_config).isFile()) {
            console.log(chalk.red('The path to google-ads.yaml you specified does not exist. You can specify a full or relative file path but it should include a file name'));
            process.exit(-1);
        }
    }
    else {
        // entering credentials one by one
        let refresh_token = '';
        const answers_new = await prompt([
            {
                type: 'input',
                name: 'googleads_config_clientid',
                message: 'OAuth client id:',
            },
            {
                type: 'input',
                name: 'googleads_config_clientsecret',
                message: 'OAuth client secret:',
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
                },
            ], answers)).googleads_config_refreshtoken;
        }
        // google-ads.yaml wasn't specified (credentials were entered), so let create it under the default name
        path_to_googleads_config = 'google-ads.yaml';
        const yaml_content = `# File was generated with create-gaarf-wf at ${new Date()}
developer_token: ${answers_new.googleads_config_devtoken}
client_id: ${answers_new.googleads_config_clientid}
client_secret: ${answers_new.googleads_config_clientsecret}
login_customer_id: ${answers_new.googleads_config_mcc}
refresh_token: ${refresh_token}
    `;
        fs.writeFileSync(path_to_googleads_config, yaml_content, {
            encoding: 'utf8',
        });
        console.log(`Google Ads API credentials were saved to ${path_to_googleads_config}`);
    }
    return path_to_googleads_config;
}
function get_answers() {
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
async function init() {
    const answers = get_answers();
    const status_log = `Running create-gaarf-wf in ${cwd}`;
    if (is_debug) {
        fs.writeFileSync(LOG_FILE, `[${new Date()}]${status_log}`);
    }
    console.log(chalk.gray(status_log));
    console.log(chalk.yellow(figlet.textSync('Gaarf Workflow', { horizontalLayout: 'full' })));
    console.log(`Welcome to interactive generator for Gaarf Workflow (${chalk.redBright('G')}oogle ${chalk.redBright('A')}ds ${chalk.redBright('A')}PI ${chalk.redBright('R')}eport ${chalk.redBright('F')}etcher Workflow)`);
    console.log('You will be asked a bunch of questions to prepare and initialize your cloud infrastructure');
    console.log('It is best to run this script in a folder that is a parent for your queries');
    const gcp_project_id = await initialize_gcp_project(answers);
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
        .find(f => path.basename(f).includes('ads') && path.basename(f).includes('queries'));
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
            name: 'custom_ids_query_path',
            message: 'Sql file path with a query to filter customer accounts (leave blank if not needed):',
            default: '',
        },
    ], answers);
    const path_to_ads_queries = answers1.path_to_ads_queries;
    const path_to_ads_queries_abs = path.join(cwd, path_to_ads_queries);
    const path_to_bq_queries = answers1.path_to_bq_queries;
    const path_to_bq_queries_abs = path.join(cwd, path_to_bq_queries);
    let gcs_bucket = answers1.gcs_bucket;
    if (!fs.existsSync(path_to_ads_queries_abs)) {
        fs.mkdirSync(path_to_ads_queries_abs);
        console.log(chalk.grey(`Created '${path_to_ads_queries_abs}' folder`));
    }
    if (!fs.existsSync(path_to_bq_queries_abs)) {
        fs.mkdirSync(path_to_bq_queries_abs);
        console.log(chalk.grey(`Created '${path_to_bq_queries_abs}' folder`));
    }
    const custom_ids_query_path = answers1.custom_ids_query_path;
    gcs_bucket = (gcs_bucket || gcp_project_id).trim();
    const path_to_googleads_config = await initialize_googleads_config(answers);
    // clone gaarf repo
    const gaarf_folder = 'ads-api-fetcher';
    if (!fs.existsSync(gaarf_folder)) {
        await exec_cmd(`git clone ${GIT_REPO} --depth 1 ${gaarf_folder}`, new clui.Spinner(`Cloning Gaarf repository (${GIT_REPO}), please wait...`));
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
        catch (_a) { }
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
    // create a bucket
    const res = await exec_cmd(`gsutil mb -b on gs://${gcs_bucket}`, new clui.Spinner(`Creating a GCS bucket ${gcs_bucket}`), { silent: true });
    if (!res.stderr.includes(`ServiceException: 409 A Cloud Storage bucket named '${gcs_bucket}' already exists`)) {
        console.log(chalk.red(`Could not create a bucket ${gcs_bucket}`));
        console.log(res.stderr);
    }
    const gcs_base_path = `gs://${gcs_bucket}/${name}`;
    // Create deploy-scripts.sh
    let deploy_custom_query_snippet = '';
    let custom_query_gcs_path;
    if (custom_ids_query_path) {
        if (!fs.existsSync(custom_ids_query_path)) {
            console.log(chalk.red(`Could not find script '${custom_ids_query_path}'`));
        }
        custom_query_gcs_path = `${gcs_base_path}/get-accounts.sql`;
        deploy_custom_query_snippet = `gsutil -m cp ${custom_ids_query_path} $GCS_BASE_PATH/get-accounts.sql`;
    }
    // Note that we deploy queries to hard-coded paths
    deploy_shell_script('deploy-scripts.sh', `# Deploy Ads and BQ scripts from local folders to Goggle Cloud Storage.
GCS_BASE_PATH=${gcs_base_path}

gsutil -m cp ${path_to_googleads_config} $GCS_BASE_PATH/google-ads.yaml
${deploy_custom_query_snippet}

gsutil rm -r $GCS_BASE_PATH/${path_to_ads_queries}
gsutil -m cp -R ./${path_to_ads_queries}/* $GCS_BASE_PATH/${PATH_ADS_QUERIES}/

gsutil rm -r $GCS_BASE_PATH/${path_to_bq_queries}
gsutil -m cp -R ./${path_to_bq_queries}/* $GCS_BASE_PATH/${PATH_BQ_QUERIES}/
`);
    const gcp_region = await get_gcp_regions(answers);
    // Create deploy-wf.sh
    const workflow_name = name + '-wf';
    const function_name = name;
    let cf_memory = (await prompt([
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
    // NOTE: Cloud Function Gen2 (in contrast to Gen1) don't support arbitrary memory size, for using >1GB you need to increase CPU
    //       but `gcloud functions` command doesn't support this.
    //       So have to deploy via `gcloud functions` with small memory and then update via `gcloud run` to update CPU and memory.
    let deploy_cf_add = '';
    if (cf_memory === '2048MB' ||
        cf_memory === '4096MB' ||
        cf_memory === '8192MB') {
        deploy_cf_add = `
gcloud run services update ${function_name} --region ${gcp_region} --cpu 1 --memory=${cf_memory.replaceAll('MB', 'Mi')}
`; //--no-cpu-throttling (add or not?)
        cf_memory = '512MB';
    }
    deploy_shell_script('deploy-wf.sh', `# Deploy Cloud Functions and Cloud Workflow
set -e
cd ./${gaarf_folder}
git pull --ff
cd ./gcp/workflow
./setup.sh --name ${workflow_name} --region ${gcp_region}
cd ./../functions
./setup.sh --name ${function_name} --memory ${cf_memory} --region ${gcp_region}
${deploy_cf_add}
`);
    const has_ads_queries = !!fs.readdirSync(path_to_ads_queries).length;
    const has_bq_queries = !!fs.readdirSync(path_to_bq_queries).length;
    const has_adsconfig = fs.existsSync(path_to_googleads_config);
    if (!has_ads_queries) {
        console.log(chalk.red(`Please place your ads scripts into '${path_to_ads_queries}' folder`));
    }
    if (!has_adsconfig) {
        console.log(chalk.red(`Please put your Ads API config into '${path_to_googleads_config}' file`));
    }
    const progress = {
        scripts_deployed: false,
        wf_created: false,
        wf_scheduled: false,
    };
    // now we need parameters for running the WF
    let ads_customer_id;
    if (fs.existsSync(path_to_googleads_config)) {
        const yamldoc = (yaml.load(fs.readFileSync(path_to_googleads_config, 'utf-8')));
        // look up for the default default value for account id (CID) from google-ads.yaml
        ads_customer_id =
            yamldoc['customer_id'] ||
                yamldoc['client_customer_id'] ||
                yamldoc['login_customer_id'];
    }
    const answers2 = await prompt([
        {
            type: 'input',
            name: 'output_dataset',
            message: 'BigQuery dataset for ads queries results ("-" will be converted to "_"):',
            default: name + '_ads',
            filter(input) {
                return input.replace(/[ -]/g, '_');
            },
        },
        {
            type: 'input',
            name: 'customer_id',
            message: 'Ads account id (customer id, or a list of ids via ","):',
            default: ads_customer_id,
        },
    ], answers);
    // now we detect macro used in queries and ask for their values
    const macro_ads = await get_macro_values(path.join(cwd, path_to_ads_queries), answers, 'ads_macro');
    const macro_bq = await get_macro_values(path.join(cwd, path_to_bq_queries), answers, 'bq_macro');
    const bq_writer_options = answers.bq_writer_options;
    const bq_location = gcp_region && gcp_region.startsWith('europe') ? 'europe' : '';
    const output_dataset = answers2.output_dataset;
    const customer_id = answers2.customer_id.toString().replaceAll('-', '');
    const wf_data = {
        cloud_function: name,
        gcs_bucket: gcs_bucket,
        location: gcp_region,
        ads_queries_path: `${name}/${PATH_ADS_QUERIES}/`,
        bq_queries_path: `${name}/${PATH_BQ_QUERIES}/`,
        dataset: output_dataset,
        cid: customer_id,
        ads_config_path: `${gcs_base_path}/google-ads.yaml`,
        customer_ids_query: custom_query_gcs_path,
        bq_dataset_location: bq_location,
        bq_writer_options: bq_writer_options,
        ads_macro: macro_ads,
        bq_macro: macro_bq,
        bq_sql: {},
    };
    // Create run-wf.sh
    deploy_shell_script('run-wf.sh', `set -e
state=$(gcloud workflows run ${workflow_name} --location=${gcp_region} \
--data='${JSON.stringify(wf_data, null, 2)}' --format="get(state)")
if [[ $state == 'FAILED' ]]; then
  echo 'Execution failed'
  exit -1
else
  echo 'Execution succeeded'
  exit 0
fi
`);
    // now execute some scripts
    // deploying queries and ads config to GCS
    if ((await prompt({
        type: 'confirm',
        name: 'deploy_scripts',
        message: 'Do you want to deploy scripts (Ads/BQ) to GCS:',
        default: true,
    }, answers)).deploy_scripts) {
        const res = await exec_cmd(path.join(cwd, './deploy-scripts.sh'), null, {
            realtime: true,
        });
        if (res.code !== 0 && !ignore_errors) {
            console.log(chalk.red('Scripts deployment (deploy-scripts.sh) failed, breaking'));
            process.exit(res.code);
        }
        progress.scripts_deployed = true;
    }
    else {
        console.log(chalk.yellow("Please note that before you deploy queries to GCS (deploy-scripts.sh) there's no sense in running workflow (it'll fail)"));
    }
    if ((await prompt({
        type: 'confirm',
        name: 'deploy_wf',
        message: 'Do you want to deploy Cloud components:',
        default: true,
    }, answers)).deploy_wf) {
        // deploying GCP components
        const res = await exec_cmd(path.join(cwd, './deploy-wf.sh'), new clui.Spinner('Deploying Cloud components, please wait...'));
        if (res.code !== 0 && !ignore_errors) {
            console.log(chalk.red('Cloud components deployment (deploy-wf.sh) failed, breaking'));
            process.exit(res.code);
        }
        progress.wf_created = true;
    }
    else {
        console.log(chalk.yellow("Please note that before you deploy cloud components (deploy-wf.sh) there's no sense in running a scheduler job"));
    }
    const answers3 = await prompt({
        //when: progress.scripts_deployed && progress.wf_created,
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
                message: "Do you want to run the job right now (it's asynchronous):",
            },
        ], answers);
        const time_parts = answers_schedule.schedule_time.split(':');
        schedule_cron = `${time_parts.length > 1 ? time_parts[1] : 0} ${time_parts[0]} * * *`;
        answers3.run_job = answers_schedule.run_job;
    }
    // Create schedule-wf.sh
    deploy_shell_script('schedule-wf.sh', `# Create Scheduler Job to execute Cloud Workflow
PROJECT_ID=${gcp_project_id}
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="csv(projectNumber)" | tail -n 1)
# be default Cloud Worflows run under the Compute Engine default service account:
SERVICE_ACCOUNT=$PROJECT_NUMBER-compute@developer.gserviceaccount.com

REGION=${gcp_region}
WORKFLOW_NAME=${workflow_name}
JOB_NAME=$WORKFLOW_NAME

data='${JSON.stringify(wf_data, null, 2).replaceAll('"', '\\"')}'

JOB_EXISTS=$(gcloud scheduler jobs list --location=$REGION --format="value(ID)" --filter="ID:'$JOB_NAME'")
if [[ -n $JOB_EXISTS ]]; then
  gcloud scheduler jobs delete $JOB_NAME --location $REGION --quiet
fi

# run the job daily at midnight
gcloud scheduler jobs create http $JOB_NAME \\
  --schedule="${schedule_cron}" \\
  --uri="https://workflowexecutions.googleapis.com/v1/projects/$PROJECT_ID/locations/$REGION/workflows/$WORKFLOW_NAME/executions" \
  --location=$REGION \\
  --message-body="{\\"argument\\": \\"$data\\"}" \\
  --oauth-service-account-email="$SERVICE_ACCOUNT" \\
  --time-zone="Etc/UTC"

#  --time-zone="TIME_ZONE" \
# timezone: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

# to run the job:
#gcloud scheduler jobs run ${workflow_name} --location=${gcp_region}
`);
    if (answers3.schedule_wf) {
        const res = await exec_cmd(path.join(cwd, './schedule-wf.sh'), new clui.Spinner('Creating a Scheduler Job, please wait...'));
        if (res.code === 0) {
            console.log('Created a Scheduler Job. You can recreate it with different settings by running schedule-wf.sh');
        }
        progress.wf_scheduled = true;
        if (answers3.run_job) {
            // running the job
            const res = await exec_cmd(`gcloud scheduler jobs run ${workflow_name} --location=${gcp_region}`, null, { realtime: true });
            if (res.code !== 0 && !ignore_errors) {
                console.log(chalk.red('Starting the Scheduler Job has failed, breaking'));
                process.exit(res.code);
            }
        }
    }
    if (!answers3.run_job) {
        // Scheduler Job wasn't run, maybe the user want to run the workflow directly (it's synchronous in contract to the scheduler)
        const answers_wf = await prompt([
            {
                type: 'confirm',
                name: 'run_wf',
                message: "Do you want to run the workflow right now (it's synchronous):",
            },
        ], answers);
        if (answers_wf.run_wf) {
            const res = await exec_cmd(path.join(cwd, './run-wf.sh'), null, { realtime: true });
            if (res.code !== 0 && !ignore_errors) {
                console.log(chalk.red('Running workflow (run-wf.sh) has failed, breaking'));
                process.exit(res.code);
            }
        }
    }
    // creating scripts for directly executing gaarf
    const ads_macro_clistr = Object.entries(macro_ads)
        .map(macro => `--macro.${macro[0]}=${macro[1]}`)
        .join(' ');
    deploy_shell_script('run-gaarf-console.sh', `${gaarf_folder}/js/gaarf ${path_to_ads_queries}/*.sql --account=${customer_id} --ads-config=${path_to_googleads_config} --output=console --console.transpose=always ${ads_macro_clistr}`);
    deploy_shell_script('run-gaarf.sh', `${gaarf_folder}/js/gaarf ${path_to_ads_queries}/*.sql --account=${customer_id} --ads-config=${path_to_googleads_config} --output=bq --bq.project=${gcp_project_id} --bq.dataset=${output_dataset} ${ads_macro_clistr}`);
    const bq_macro_clistr = Object.entries(macro_bq)
        .map(macro => `--macro.${macro[0]}=${macro[1]}`)
        .join(' ');
    deploy_shell_script('run-gaarf-bq.sh', `${gaarf_folder}/js/gaarf-bq ${path_to_bq_queries}/*.sql --project=${gcp_project_id} ${bq_macro_clistr}`);
    // clone dashboard
    if ((await prompt({
        type: 'confirm',
        name: 'clone_dashboard',
        message: 'Do you want to clone a Looker Studio dashboard:',
        default: false,
    }, answers)).clone_dashboard) {
        await deploy_dashboard(answers, gcp_project_id, output_dataset, macro_bq);
        await exec_cmd(`gsutil cp ${DASHBOARD_LINK_FILE} ${gcs_base_path}/`, new clui.Spinner(`Copying ${DASHBOARD_LINK_FILE} to GCS ${gcs_base_path}/`), { silent: true });
    }
    // at last stage we'll copy all shell scripts to same GCS bucket in scrips folders, so another users could manage the project easily
    await exec_cmd(`gsutil -m cp *.sh ${gcs_base_path}/scripts/`, new clui.Spinner(`Copying all shell scripts to GCS ${gcs_base_path}/scripts`), { silent: true });
    console.log(`All generated shell scripts were uploaded to GCS ${chalk.cyan(gcs_base_path + '/scripts')}`);
    console.log(chalk.green('All done'));
    console.log(chalk.yellow('Tips for using the generated scripts:'));
    console.log(` 🔹 ${chalk.cyan('deploy-scripts.sh')} - redeploy queries and google-ads.yaml to GCS`);
    console.log(` 🔹 ${chalk.cyan('deploy-wf.sh')} - redeploy Cloud Functions and Workflow`);
    console.log(` 🔹 ${chalk.cyan('run-wf.sh')} - execute workflow directly, see arguments inside`);
    console.log(` 🔹 ${chalk.cyan('schedule-wf.sh')} - reschedule workflow execution, see arguments inside`);
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