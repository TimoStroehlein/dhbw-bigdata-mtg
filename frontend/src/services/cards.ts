import axios from 'axios';

export const getCards = async (searchParam: string): Promise<Array<any>> => {
    const response = await axios.get('http://localhost:3031/cards', {
        params: {
            searchParam: searchParam
        }
    });
    return response.data['cards'];
}
