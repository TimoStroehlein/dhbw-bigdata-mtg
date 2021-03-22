import {Container, Content, Header} from 'rsuite';
import React from 'react';
import './styles.scss';

export const SettingsScreen = (): JSX.Element => {
    return (
        <Container>
            <Header>
                <h2>Settings</h2>
            </Header>
            <Content>
                <p>Content</p>
            </Content>
        </Container>
    );
}
