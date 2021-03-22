import {Container, Content, Header} from 'rsuite';
import React from 'react';
import './styles.scss';

export const HelpScreen = (): JSX.Element => {
    return (
        <Container>
            <Header>
                <h2>Help</h2>
            </Header>
            <Content>
                <p>Content</p>
            </Content>
        </Container>
    );
}
