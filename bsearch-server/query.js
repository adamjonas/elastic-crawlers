const AppSearchClient = require('@elastic/app-search-node')

const levenshtein = require('./levenshtein')

const stackoverflow = require('./stackoverflow')

const apiKey = process.env.API_KEY
const baseUrlFn = () => process.env.BASE_URL_FN
const client = new AppSearchClient(undefined, apiKey, baseUrlFn)

const engineName = process.env.ENGINE_NAME
const resultFields = { id: { raw: {} }, title: { raw: {} }, body_content: { raw: {} }, domains: { raw: {} }, url: { raw: {} } }

async function search(query) {

    const searchFields = { title : {}, body_content: {} }
    const options = { search_fields: searchFields, result_fields: resultFields, page: { size: 100 } }

    try {
        let crawlerResponse = await client.search(engineName, query, options)
        let crawlerItems = handleResponse(crawlerResponse);

        let stackoverflowResponse = await stackoverflow.search(query)

        return {
            crawler_items: crawlerItems,
            stack_overflow_items: stackoverflowResponse,
        };
    } catch (error) {
        console.error(error);
    }
}

async function searchByTitle(query) {

    const searchFields = { title : {} }
    const options = { search_fields: searchFields, result_fields: resultFields, page: { size: 100 } }

    try {
        var response = await client.search(engineName, query, options)
        return handleResponseEmail(response, query)
    } catch (error) {
        console.error(error);
    }
}

function handleResponse(response) {

    if (response.meta.page.size == 0) return []

    let result = []

    for(let doc of response.results) {

        if (doc.domains.raw[0] == "https://bitcoin.stackexchange.com") {
            continue;
        }

        var item = {};
        item.id = doc.id.raw;
        item.url = doc.url.raw;
        item.title = doc.title.raw;
        item.body_content = doc.body_content.raw;
        item.domain = doc.domains.raw[0];

        item.is_email_thread = item.domain === 'https://lists.linuxfoundation.org';

        // TODO: "[Lightning-dev] [bitcoin-dev] OP_CAT was Re: Continuing the discussion about noinput / anyprevout"
        // and "[bitcoin-dev] [Lightning-dev] OP_CAT was Re: Continuing the discussion about noinput / anyprevout"
        // should be considered same subject

        if (!item.is_email_thread || !result.find(o => o.title === item.title && o.domain === item.domain)) {
            result.push(item)
        }

        /*let same_thread_item = result.find(o => o.title === item.title && o.domain === item.domain);
        if (same_thread_item) {
            same_thread_item.same_thread.push(item)
        } else {
            result.push(item)
        }*/
    }

    return result
}



function handleResponseEmail(response, query) {

    if (response.meta.page.size == 0) return []

    let result = []

    for(let doc of response.results) {
        
        var item = {};
        item.id = doc.id.raw;
        item.url = doc.url.raw;
        item.title = doc.title.raw;
        item.body_content = doc.body_content.raw;
        item.domain = doc.domains.raw[0];

        const title = item.title.replace(/Enc:/gi, '').replace(/Re:/gi, '')
        const q = query.replace(/Enc:/gi, '').replace(/Re:/gi, '')

        if (levenshtein(title, q) < 6) {
            result.push(item)
        } 
    }

    return result
}

module.exports = {
    search,
    searchByTitle
}
