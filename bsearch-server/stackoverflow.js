const axios = require('axios')

async function search(search_term) {

    let results = [];

    try {
        let res = await axios
            .get(`https://api.stackexchange.com/2.3/search?order=desc&sort=activity&intitle=${search_term}&site=bitcoin&filter=!nKzQUR30SM`);

        if (res.status == 200) {
            for (item of res.data.items) {
                results.push({
                    id: item.question_id,
                    title: item.title,
                    body_content: item.body_markdown,
                    url: item.link,
                    last_activity_date: item.last_activity_date,
                    creation_date: item.creation_date,
                    score: item.score,
                    domain: "https://bitcoin.stackexchange.com"
                })
            }
        }
    }
    catch (error) {
        console.error(error);
    }

    return results;
}

async function test() {
    let items = await querySOV("transaction");
    console.log(items);
    console.log(items.length);
}

module.exports = {
    search
}
