import React, {useState} from 'react';
import {BrowserRouter as Router, Link, Route} from 'react-router-dom';
import {Container, Dropdown, Icon, Nav, Navbar, Sidebar, Sidenav} from 'rsuite';
import './styles.scss';
import {SettingsScreen} from '../screens/Settings';
import {HomeScreen} from '../screens/Home';
import {HelpScreen} from '../screens/Help';
import {SASTVulnerability1Screen} from '../screens/SAST/Vulnerability1';
import {SASTVulnerability2Screen} from '../screens/SAST/Vulnerability2';
import {SASTVulnerability3Screen} from '../screens/SAST/Vulnerability3';
import {DASTVulnerability1Screen} from '../screens/DAST/Vulnerability1';
import {DASTVulnerability2Screen} from '../screens/DAST/Vulnerability2';
import {DASTVulnerability3Screen} from '../screens/DAST/Vulnerability3';

const Routes = (): JSX.Element => {
    return (
        <Container className="content">
            <Route exact path="/">
                <HomeScreen/>
            </Route>
            <Route path="/sast/vulnerability-1">
                <SASTVulnerability1Screen/>
            </Route>
            <Route path="/sast/vulnerability-2">
                <SASTVulnerability2Screen/>
            </Route>
            <Route path="/sast/vulnerability-3">
                <SASTVulnerability3Screen/>
            </Route>
            <Route path="/dast/vulnerability-1">
                <DASTVulnerability1Screen/>
            </Route>
            <Route path="/dast/vulnerability-2">
                <DASTVulnerability2Screen/>
            </Route>
            <Route path="/dast/vulnerability-3">
                <DASTVulnerability3Screen/>
            </Route>
            <Route path="/settings">
                <SettingsScreen/>
            </Route>
            <Route path="/help">
                <HelpScreen/>
            </Route>
        </Container>
    );
}

const NavigationToggle = ({ expand, onChange }: {expand: boolean, onChange: () => void}): JSX.Element => {
    return (
        <Navbar appearance="subtle" className="nav-toggle">
            <Navbar.Body>
                <Nav>
                    <Dropdown
                        placement="topStart"
                        trigger="click"
                        renderTitle={() => {
                            return <Icon className="icon" icon="cog" />;
                        }}
                    >
                        <Dropdown.Item componentClass={Link} to="/help">Help</Dropdown.Item>
                        <Dropdown.Item componentClass={Link} to="/settings">Settings</Dropdown.Item>
                    </Dropdown>
                </Nav>

                <Nav pullRight>
                    <Nav.Item className="nav-toggle-item" onClick={onChange}>
                        <Icon icon={expand ? 'angle-left' : 'angle-right'} />
                    </Nav.Item>
                </Nav>
            </Navbar.Body>
        </Navbar>
    );
};

export const NavigationSidebar = (): JSX.Element => {
    const [expanded, setExpanded] = useState(true);
    const [activeKey, setActiveKey] = useState(1);

    return (
        <div>
            <Container>
                <Router>
                    <Sidebar
                        className="sidebar"
                        width={expanded ? 260 : 56}
                        collapsible
                    >
                        <Sidenav
                            expanded={expanded}
                            defaultOpenKeys={['3']}
                            appearance="subtle"
                            activeKey={activeKey}
                            onSelect={(eventKey) => {setActiveKey(eventKey)}}
                        >
                            <Sidenav.Header>
                                <div className="header">
                                    <Icon className="header-icon" icon="user-secret" size="lg" />
                                    <span className="header-span">DHBW T3100</span>
                                </div>
                            </Sidenav.Header>
                            <Sidenav.Body>
                                <Nav>
                                    <Nav.Item componentClass={Link} to="/" eventKey="1" active icon={<Icon icon="home" />}>
                                        Home
                                    </Nav.Item>
                                    <Dropdown
                                        eventKey="3"
                                        trigger="hover"
                                        title="SAST"
                                        icon={<Icon icon="code" />}
                                        placement="rightStart"
                                    >
                                        <Dropdown.Item componentClass={Link} to="/sast/vulnerability-1" eventKey="2-1">
                                            Vulnerability 1
                                        </Dropdown.Item>
                                        <Dropdown.Item componentClass={Link} to="/sast/vulnerability-2" eventKey="2-2">
                                            Vulnerability 2
                                        </Dropdown.Item>
                                        <Dropdown.Item componentClass={Link} to="/sast/vulnerability-3" eventKey="2-3">
                                            Vulnerability 3
                                        </Dropdown.Item>
                                    </Dropdown>
                                    <Dropdown
                                        eventKey="3"
                                        trigger="hover"
                                        title="DAST"
                                        icon={<Icon icon="shield" />}
                                        placement="rightStart"
                                    >
                                        <Dropdown.Item componentClass={Link} to="/dast/vulnerability-1" eventKey="3-1">
                                            Vulnerability 1
                                        </Dropdown.Item>
                                        <Dropdown.Item componentClass={Link} to="/dast/vulnerability-2" eventKey="3-2">
                                            Vulnerability 2
                                        </Dropdown.Item>
                                        <Dropdown.Item componentClass={Link} to="/dast/vulnerability-3" eventKey="3-3">
                                            Vulnerability 3
                                        </Dropdown.Item>
                                    </Dropdown>
                                </Nav>
                            </Sidenav.Body>
                        </Sidenav>
                        <NavigationToggle expand={expanded} onChange={() => setExpanded(!expanded)} />
                    </Sidebar>
                    <Routes/>
                </Router>
            </Container>
        </div>
    );
}
