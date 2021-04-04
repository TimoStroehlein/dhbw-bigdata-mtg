import axios from 'axios';

/**
 * Get the cards correponding to the search param.
 * @param searchParam Search paramteter to filter the cards (card name, text or artist).
 * @returns Filtered cards.
 */
export const getCards = async (searchParam: string): Promise<Array<any>> => {
    const response = await axios.get('http://localhost:3031/cards', {
        params: {
            searchParam: searchParam
        }
    });
    return response.data['cards'];
}
