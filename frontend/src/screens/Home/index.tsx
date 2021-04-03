import {Container, Content, FlexboxGrid, Grid, Header, Icon, Input, InputGroup, Row, List} from 'rsuite';
import './styles.scss';
import { getCards } from '../../services/cards';
import { useEffect, useState } from 'react';
import { Card } from '../../models/card';

export const HomeScreen = (): JSX.Element => {
    const [cards, setCards] = useState([] as Array<Card>);
    const [searchParam, setSearchParam] = useState('');

    useEffect(() => {
        const fetchCards = async () => {
            const cards = await getCards(searchParam);
            setCards(cards);
        }
        fetchCards();
    }, [searchParam]);

    return (
        <Container>
            <Header>
                <Grid>
                    <Row className="header-title">
                        <h2>Big Data - MTG API</h2>
                    </Row>
                    <Row>
                        <FlexboxGrid align="middle" justify="center" className="header-grid">
                            <FlexboxGrid.Item>
                                <InputGroup className="search">
                                    <Input onChange={(value) => setSearchParam(value)} />
                                    <InputGroup.Button>
                                        <Icon icon="search" />
                                    </InputGroup.Button>
                                </InputGroup>
                            </FlexboxGrid.Item>
                        </FlexboxGrid>
                    </Row>
                </Grid>
                
            </Header>
            <Content className="content">
                <List>
                    <List.Item>
                        <FlexboxGrid>
                                <FlexboxGrid.Item colspan={6}>
                                    Image
                                </FlexboxGrid.Item>
                                <FlexboxGrid.Item colspan={2}>
                                    Name
                                </FlexboxGrid.Item>
                                <FlexboxGrid.Item colspan={2}>
                                    Subtypes
                                </FlexboxGrid.Item>
                                <FlexboxGrid.Item colspan={4}>
                                    Text
                                </FlexboxGrid.Item>
                                <FlexboxGrid.Item colspan={4}>
                                    Flavor
                                </FlexboxGrid.Item>
                                <FlexboxGrid.Item colspan={4}>
                                    Artist
                                </FlexboxGrid.Item>
                                <FlexboxGrid.Item colspan={2}>
                                    Multiverseid
                                </FlexboxGrid.Item>
                            </FlexboxGrid>
                    </List.Item>
                    {cards.map((card) => (
                        <List.Item>
                            <FlexboxGrid>
                                <FlexboxGrid.Item colspan={6}>
                                    <img src={card.imageUrl}></img>
                                </FlexboxGrid.Item>
                                <FlexboxGrid.Item colspan={2}>
                                    {card.name}
                                </FlexboxGrid.Item>
                                <FlexboxGrid.Item colspan={2}>
                                    {card.subtypes}
                                </FlexboxGrid.Item>
                                <FlexboxGrid.Item colspan={4}>
                                    {card.text}
                                </FlexboxGrid.Item>
                                <FlexboxGrid.Item colspan={4}>
                                    {card.flavor}
                                </FlexboxGrid.Item>
                                <FlexboxGrid.Item colspan={4}>
                                    {card.artist}
                                </FlexboxGrid.Item>
                                <FlexboxGrid.Item colspan={2}>
                                    {card.multiverseid}
                                </FlexboxGrid.Item>
                            </FlexboxGrid>
                        </List.Item>
                    ))}
                </List>                
            </Content>
        </Container>
    );
}
