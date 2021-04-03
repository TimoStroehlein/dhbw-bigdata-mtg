import {Container, Content, FlexboxGrid, Grid, Header, Icon, Input, InputGroup, Row} from 'rsuite';
import './styles.scss';
import { getCards } from '../../services/database';

export const HomeScreen = (): JSX.Element => {
    const card = getCards();

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
                                    <Input />
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
                <p>Content</p>
            </Content>
        </Container>
    );
}
